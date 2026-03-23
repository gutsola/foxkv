mod encoder;
mod replay;

use std::fs::{File, OpenOptions};
use std::io;
use std::io::Write;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use crate::command::SetCondition;
use crate::config::model::{AofConfig, AppendFsyncPolicy};

pub use replay::{replay_commands, replay_set_commands};

#[derive(Debug, Clone)]
pub struct AofRuntimeConfig {
    pub enabled: bool,
    pub file_path: PathBuf,
    pub appendfsync: AppendFsyncPolicy,
    pub auto_rewrite_percentage: u32,
    pub auto_rewrite_min_size_bytes: u64,
    pub use_rdb_preamble: bool,
}

#[derive(Clone)]
pub struct AofEngine {
    inner: Arc<AofInner>,
}

struct AofInner {
    config: AofRuntimeConfig,
    file: Mutex<File>,
}

impl AofRuntimeConfig {
    pub fn from_config(base_dir: &std::path::Path, cfg: &AofConfig) -> Self {
        Self {
            enabled: cfg.enabled,
            file_path: base_dir.join(&cfg.appendfilename),
            appendfsync: cfg.appendfsync,
            auto_rewrite_percentage: cfg.auto_rewrite_percentage,
            auto_rewrite_min_size_bytes: cfg.auto_rewrite_min_size_bytes,
            use_rdb_preamble: cfg.use_rdb_preamble,
        }
    }
}

impl AofEngine {
    pub fn open(config: AofRuntimeConfig) -> io::Result<Self> {
        if let Some(parent) = config.file_path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(&config.file_path)?;
        let engine = Self {
            inner: Arc::new(AofInner {
                config,
                file: Mutex::new(file),
            }),
        };
        engine.start_background_fsync();
        Ok(engine)
    }

    pub fn append_set(
        &self,
        key: &[u8],
        value: &[u8],
        ttl_ms: Option<u64>,
        condition: SetCondition,
    ) -> io::Result<()> {
        self.append_encoded(encoder::encode_set_command(key, value, ttl_ms, condition))
    }

    pub fn append_setnx(&self, key: &[u8], value: &[u8]) -> io::Result<()> {
        self.append_command(b"SETNX", &[key, value])
    }

    pub fn append_getset(&self, key: &[u8], value: &[u8]) -> io::Result<()> {
        self.append_command(b"GETSET", &[key, value])
    }

    pub fn append_append(&self, key: &[u8], value: &[u8]) -> io::Result<()> {
        self.append_command(b"APPEND", &[key, value])
    }

    pub fn append_incr(&self, key: &[u8]) -> io::Result<()> {
        self.append_command(b"INCR", &[key])
    }

    pub fn append_incrby(&self, key: &[u8], increment: &[u8]) -> io::Result<()> {
        self.append_command(b"INCRBY", &[key, increment])
    }

    pub fn append_incrbyfloat(&self, key: &[u8], increment: &[u8]) -> io::Result<()> {
        self.append_command(b"INCRBYFLOAT", &[key, increment])
    }

    pub fn append_decr(&self, key: &[u8]) -> io::Result<()> {
        self.append_command(b"DECR", &[key])
    }

    pub fn append_decrby(&self, key: &[u8], decrement: &[u8]) -> io::Result<()> {
        self.append_command(b"DECRBY", &[key, decrement])
    }

    pub fn append_setrange(&self, key: &[u8], offset: &[u8], value: &[u8]) -> io::Result<()> {
        self.append_command(b"SETRANGE", &[key, offset, value])
    }

    pub fn append_mset(&self, pairs: &[(&[u8], &[u8])]) -> io::Result<()> {
        self.append_encoded(encoder::encode_pairs_command(b"MSET", pairs))
    }

    pub fn append_mset_args(&self, args: &[&[u8]]) -> io::Result<()> {
        self.append_command(b"MSET", args)
    }

    pub fn append_msetnx(&self, pairs: &[(&[u8], &[u8])]) -> io::Result<()> {
        self.append_encoded(encoder::encode_pairs_command(b"MSETNX", pairs))
    }

    pub fn append_msetnx_args(&self, args: &[&[u8]]) -> io::Result<()> {
        self.append_command(b"MSETNX", args)
    }

    pub fn append_del(&self, keys: &[&[u8]]) -> io::Result<()> {
        self.append_command(b"DEL", keys)
    }

    pub fn append_flushall(&self) -> io::Result<()> {
        self.append_command(b"FLUSHALL", &[])
    }

    pub fn append_expire(&self, key: &[u8], seconds: &[u8]) -> io::Result<()> {
        self.append_command(b"EXPIRE", &[key, seconds])
    }

    pub fn append_expireat(&self, key: &[u8], timestamp: &[u8]) -> io::Result<()> {
        self.append_command(b"EXPIREAT", &[key, timestamp])
    }

    pub fn append_pexpire(&self, key: &[u8], ms: &[u8]) -> io::Result<()> {
        self.append_command(b"PEXPIRE", &[key, ms])
    }

    pub fn append_pexpireat(&self, key: &[u8], timestamp_ms: &[u8]) -> io::Result<()> {
        self.append_command(b"PEXPIREAT", &[key, timestamp_ms])
    }

    pub fn append_persist(&self, key: &[u8]) -> io::Result<()> {
        self.append_command(b"PERSIST", &[key])
    }

    fn start_background_fsync(&self) {
        if !matches!(self.inner.config.appendfsync, AppendFsyncPolicy::EverySec) {
            return;
        }
        let this = self.clone();
        std::thread::spawn(move || {
            loop {
                std::thread::sleep(Duration::from_secs(1));
                if let Err(err) = this.sync_data() {
                    eprintln!("aof everysec fsync failed: {err}");
                }
            }
        });
    }

    pub fn sync_data(&self) -> io::Result<()> {
        let file = self
            .inner
            .file
            .lock()
            .map_err(|_| io::Error::other("aof mutex poisoned"))?;
        file.sync_data()
    }

    fn append_encoded(&self, payload: Vec<u8>) -> io::Result<()> {
        let mut file = self
            .inner
            .file
            .lock()
            .map_err(|_| io::Error::other("aof mutex poisoned"))?;
        file.write_all(&payload)?;
        match self.inner.config.appendfsync {
            AppendFsyncPolicy::Always => file.sync_data(),
            AppendFsyncPolicy::EverySec | AppendFsyncPolicy::No => Ok(()),
        }?;
        Ok(())
    }

    fn append_command(&self, cmd: &[u8], args: &[&[u8]]) -> io::Result<()> {
        self.append_encoded(encoder::encode_command(cmd, args))
    }
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::PathBuf;

    use super::*;
    use crate::command::SetCondition;

    fn temp_aof_path(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!("foxkv_test_{}.aof", name))
    }

    fn cleanup(path: &PathBuf) {
        let _ = fs::remove_file(path);
    }

    fn create_test_engine(path: &PathBuf) -> AofEngine {
        cleanup(path);
        let config = AofRuntimeConfig {
            enabled: true,
            file_path: path.clone(),
            appendfsync: AppendFsyncPolicy::No,
            auto_rewrite_percentage: 0,
            auto_rewrite_min_size_bytes: 0,
            use_rdb_preamble: false,
        };
        AofEngine::open(config).expect("failed to open aof engine")
    }

    #[test]
    fn aof_runtime_config_from_config_converts_correctly() {
        let base_dir = std::path::Path::new("/data");
        let cfg = AofConfig {
            enabled: true,
            appendfilename: "appendonly.aof".to_string(),
            appendfsync: AppendFsyncPolicy::EverySec,
            auto_rewrite_percentage: 100,
            auto_rewrite_min_size_bytes: 64 * 1024 * 1024,
            use_rdb_preamble: true,
            aof_rewrite_incremental_fsync: true,
        };
        let runtime = AofRuntimeConfig::from_config(base_dir, &cfg);
        assert!(runtime.enabled);
        assert_eq!(runtime.file_path, PathBuf::from("/data/appendonly.aof"));
        assert_eq!(runtime.appendfsync, AppendFsyncPolicy::EverySec);
        assert_eq!(runtime.auto_rewrite_percentage, 100);
        assert_eq!(runtime.auto_rewrite_min_size_bytes, 64 * 1024 * 1024);
        assert!(runtime.use_rdb_preamble);
    }

    #[test]
    fn aof_engine_open_creates_file_if_not_exists() {
        let path = temp_aof_path("open_creates");
        cleanup(&path);
        let _engine = create_test_engine(&path);
        assert!(path.exists());
        cleanup(&path);
    }

    #[test]
    fn aof_engine_append_set_writes_to_file() {
        let path = temp_aof_path("append_set");
        let engine = create_test_engine(&path);
        engine
            .append_set(b"mykey", b"myvalue", None, SetCondition::None)
            .unwrap();
        engine.sync_data().unwrap();
        let contents = fs::read(&path).unwrap();
        assert!(contents.starts_with(b"*3\r\n$3\r\nSET\r\n$5\r\nmykey\r\n$7\r\nmyvalue\r\n"));
        cleanup(&path);
    }

    #[test]
    fn aof_engine_append_set_with_ttl_writes_px_option() {
        let path = temp_aof_path("append_set_ttl");
        let engine = create_test_engine(&path);
        engine
            .append_set(b"key", b"val", Some(5000), SetCondition::None)
            .unwrap();
        engine.sync_data().unwrap();
        let contents = fs::read(&path).unwrap();
        assert!(contents.windows(2).any(|w| w == b"PX"));
        cleanup(&path);
    }

    #[test]
    fn aof_engine_append_set_with_condition_writes_nx_or_xx() {
        let path = temp_aof_path("append_set_nx");
        let engine = create_test_engine(&path);
        engine
            .append_set(b"key", b"val", None, SetCondition::Nx)
            .unwrap();
        engine.sync_data().unwrap();
        let contents = fs::read(&path).unwrap();
        assert!(contents.windows(2).any(|w| w == b"NX"));
        cleanup(&path);

        let path = temp_aof_path("append_set_xx");
        let engine = create_test_engine(&path);
        engine
            .append_set(b"key", b"val", None, SetCondition::Xx)
            .unwrap();
        engine.sync_data().unwrap();
        let contents = fs::read(&path).unwrap();
        assert!(contents.windows(2).any(|w| w == b"XX"));
        cleanup(&path);
    }

    #[test]
    fn aof_engine_append_setnx_writes_correct_command() {
        let path = temp_aof_path("append_setnx");
        let engine = create_test_engine(&path);
        engine.append_setnx(b"key", b"value").unwrap();
        engine.sync_data().unwrap();
        let contents = fs::read(&path).unwrap();
        assert!(contents.starts_with(b"*3\r\n$5\r\nSETNX\r\n"));
        cleanup(&path);
    }

    #[test]
    fn aof_engine_append_getset_writes_correct_command() {
        let path = temp_aof_path("append_getset");
        let engine = create_test_engine(&path);
        engine.append_getset(b"key", b"newvalue").unwrap();
        engine.sync_data().unwrap();
        let contents = fs::read(&path).unwrap();
        assert!(contents.starts_with(b"*3\r\n$6\r\nGETSET\r\n"));
        cleanup(&path);
    }

    #[test]
    fn aof_engine_append_append_writes_correct_command() {
        let path = temp_aof_path("append_append");
        let engine = create_test_engine(&path);
        engine.append_append(b"key", b"suffix").unwrap();
        engine.sync_data().unwrap();
        let contents = fs::read(&path).unwrap();
        assert!(contents.starts_with(b"*3\r\n$6\r\nAPPEND\r\n"));
        cleanup(&path);
    }

    #[test]
    fn aof_engine_append_incr_writes_correct_command() {
        let path = temp_aof_path("append_incr");
        let engine = create_test_engine(&path);
        engine.append_incr(b"counter").unwrap();
        engine.sync_data().unwrap();
        let contents = fs::read(&path).unwrap();
        assert!(contents.starts_with(b"*2\r\n$4\r\nINCR\r\n"));
        cleanup(&path);
    }

    #[test]
    fn aof_engine_append_incrby_writes_correct_command() {
        let path = temp_aof_path("append_incrby");
        let engine = create_test_engine(&path);
        engine.append_incrby(b"counter", b"10").unwrap();
        engine.sync_data().unwrap();
        let contents = fs::read(&path).unwrap();
        assert!(contents.starts_with(b"*3\r\n$6\r\nINCRBY\r\n"));
        cleanup(&path);
    }

    #[test]
    fn aof_engine_append_incrbyfloat_writes_correct_command() {
        let path = temp_aof_path("append_incrbyfloat");
        let engine = create_test_engine(&path);
        engine.append_incrbyfloat(b"counter", b"2.5").unwrap();
        engine.sync_data().unwrap();
        let contents = fs::read(&path).unwrap();
        assert!(contents.starts_with(b"*3\r\n$11\r\nINCRBYFLOAT\r\n"));
        cleanup(&path);
    }

    #[test]
    fn aof_engine_append_decr_writes_correct_command() {
        let path = temp_aof_path("append_decr");
        let engine = create_test_engine(&path);
        engine.append_decr(b"counter").unwrap();
        engine.sync_data().unwrap();
        let contents = fs::read(&path).unwrap();
        assert!(contents.starts_with(b"*2\r\n$4\r\nDECR\r\n"));
        cleanup(&path);
    }

    #[test]
    fn aof_engine_append_decrby_writes_correct_command() {
        let path = temp_aof_path("append_decrby");
        let engine = create_test_engine(&path);
        engine.append_decrby(b"counter", b"5").unwrap();
        engine.sync_data().unwrap();
        let contents = fs::read(&path).unwrap();
        assert!(contents.starts_with(b"*3\r\n$6\r\nDECRBY\r\n"));
        cleanup(&path);
    }

    #[test]
    fn aof_engine_append_setrange_writes_correct_command() {
        let path = temp_aof_path("append_setrange");
        let engine = create_test_engine(&path);
        engine.append_setrange(b"key", b"0", b"value").unwrap();
        engine.sync_data().unwrap();
        let contents = fs::read(&path).unwrap();
        assert!(contents.starts_with(b"*4\r\n$8\r\nSETRANGE\r\n"));
        cleanup(&path);
    }

    #[test]
    fn aof_engine_append_mset_writes_correct_command() {
        let path = temp_aof_path("append_mset");
        let engine = create_test_engine(&path);
        engine
            .append_mset(&[(b"k1", b"v1"), (b"k2", b"v2")])
            .unwrap();
        engine.sync_data().unwrap();
        let contents = fs::read(&path).unwrap();
        assert!(contents.starts_with(b"*5\r\n$4\r\nMSET\r\n"));
        cleanup(&path);
    }

    #[test]
    fn aof_engine_append_mset_args_writes_correct_command() {
        let path = temp_aof_path("append_mset_args");
        let engine = create_test_engine(&path);
        engine
            .append_mset_args(&[b"k1", b"v1", b"k2", b"v2"])
            .unwrap();
        engine.sync_data().unwrap();
        let contents = fs::read(&path).unwrap();
        assert!(contents.starts_with(b"*5\r\n$4\r\nMSET\r\n"));
        cleanup(&path);
    }

    #[test]
    fn aof_engine_append_msetnx_writes_correct_command() {
        let path = temp_aof_path("append_msetnx");
        let engine = create_test_engine(&path);
        engine.append_msetnx(&[(b"k1", b"v1")]).unwrap();
        engine.sync_data().unwrap();
        let contents = fs::read(&path).unwrap();
        assert!(contents.starts_with(b"*3\r\n$6\r\nMSETNX\r\n"));
        cleanup(&path);
    }

    #[test]
    fn aof_engine_append_msetnx_args_writes_correct_command() {
        let path = temp_aof_path("append_msetnx_args");
        let engine = create_test_engine(&path);
        engine.append_msetnx_args(&[b"k1", b"v1"]).unwrap();
        engine.sync_data().unwrap();
        let contents = fs::read(&path).unwrap();
        assert!(contents.starts_with(b"*3\r\n$6\r\nMSETNX\r\n"));
        cleanup(&path);
    }

    #[test]
    fn aof_engine_append_del_writes_correct_command() {
        let path = temp_aof_path("append_del");
        let engine = create_test_engine(&path);
        engine.append_del(&[b"key1", b"key2"]).unwrap();
        engine.sync_data().unwrap();
        let contents = fs::read(&path).unwrap();
        assert!(contents.starts_with(b"*3\r\n$3\r\nDEL\r\n"));
        cleanup(&path);
    }

    #[test]
    fn aof_engine_append_flushall_writes_correct_command() {
        let path = temp_aof_path("append_flushall");
        let engine = create_test_engine(&path);
        engine.append_flushall().unwrap();
        engine.sync_data().unwrap();
        let contents = fs::read(&path).unwrap();
        assert!(
            contents.starts_with(b"*1\r\n$8\r\nFLUSHALL\r\n"),
            "actual contents: {:?}",
            String::from_utf8_lossy(&contents)
        );
        cleanup(&path);
    }

    #[test]
    fn aof_engine_append_expire_writes_correct_command() {
        let path = temp_aof_path("append_expire");
        let engine = create_test_engine(&path);
        engine.append_expire(b"key", b"60").unwrap();
        engine.sync_data().unwrap();
        let contents = fs::read(&path).unwrap();
        assert!(contents.starts_with(b"*3\r\n$6\r\nEXPIRE\r\n"));
        cleanup(&path);
    }

    #[test]
    fn aof_engine_append_expireat_writes_correct_command() {
        let path = temp_aof_path("append_expireat");
        let engine = create_test_engine(&path);
        engine.append_expireat(b"key", b"1700000000").unwrap();
        engine.sync_data().unwrap();
        let contents = fs::read(&path).unwrap();
        assert!(contents.starts_with(b"*3\r\n$8\r\nEXPIREAT\r\n"));
        cleanup(&path);
    }

    #[test]
    fn aof_engine_append_pexpire_writes_correct_command() {
        let path = temp_aof_path("append_pexpire");
        let engine = create_test_engine(&path);
        engine.append_pexpire(b"key", b"5000").unwrap();
        engine.sync_data().unwrap();
        let contents = fs::read(&path).unwrap();
        assert!(contents.starts_with(b"*3\r\n$7\r\nPEXPIRE\r\n"));
        cleanup(&path);
    }

    #[test]
    fn aof_engine_append_pexpireat_writes_correct_command() {
        let path = temp_aof_path("append_pexpireat");
        let engine = create_test_engine(&path);
        engine.append_pexpireat(b"key", b"1700000000000").unwrap();
        engine.sync_data().unwrap();
        let contents = fs::read(&path).unwrap();
        assert!(contents.starts_with(b"*3\r\n$9\r\nPEXPIREAT\r\n"));
        cleanup(&path);
    }

    #[test]
    fn aof_engine_append_persist_writes_correct_command() {
        let path = temp_aof_path("append_persist");
        let engine = create_test_engine(&path);
        engine.append_persist(b"key").unwrap();
        engine.sync_data().unwrap();
        let contents = fs::read(&path).unwrap();
        assert!(contents.starts_with(b"*2\r\n$7\r\nPERSIST\r\n"));
        cleanup(&path);
    }

    #[test]
    fn aof_engine_multiple_appends_accumulate_in_file() {
        let path = temp_aof_path("multiple_appends");
        let engine = create_test_engine(&path);
        engine
            .append_set(b"k1", b"v1", None, SetCondition::None)
            .unwrap();
        engine
            .append_set(b"k2", b"v2", None, SetCondition::None)
            .unwrap();
        engine.append_del(&[b"k1"]).unwrap();
        engine.sync_data().unwrap();
        let contents = fs::read(&path).unwrap();
        assert!(contents.windows(3).any(|w| w == b"SET"));
        assert!(contents.windows(3).any(|w| w == b"DEL"));
        cleanup(&path);
    }

    #[test]
    fn aof_engine_sync_data_succeeds_after_write() {
        let path = temp_aof_path("sync_data");
        let engine = create_test_engine(&path);
        engine
            .append_set(b"key", b"value", None, SetCondition::None)
            .unwrap();
        let result = engine.sync_data();
        assert!(result.is_ok());
        cleanup(&path);
    }
}
