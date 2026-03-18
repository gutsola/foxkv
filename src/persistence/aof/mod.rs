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

    fn sync_data(&self) -> io::Result<()> {
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
