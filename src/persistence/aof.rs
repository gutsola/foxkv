use std::fs::{File, OpenOptions};
use std::io;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use crate::command::{Command, SetCondition, parse_command};
use crate::config::model::{AofConfig, AppendFsyncPolicy};
use crate::storage::ConcurrentMapDb;

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
        self.append_encoded(encode_set_command(key, value, ttl_ms, condition))
    }

    pub fn append_setnx(&self, key: &[u8], value: &[u8]) -> io::Result<()> {
        self.append_command(b"SETNX", &[key, value])
    }

    pub fn append_getset(&self, key: &[u8], value: &[u8]) -> io::Result<()> {
        self.append_command(b"GETSET", &[key, value])
    }

    pub fn append_mset(&self, pairs: &[(&[u8], &[u8])]) -> io::Result<()> {
        self.append_pairs_command(b"MSET", pairs)
    }

    pub fn append_msetnx(&self, pairs: &[(&[u8], &[u8])]) -> io::Result<()> {
        self.append_pairs_command(b"MSETNX", pairs)
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
        self.append_encoded(encode_command(cmd, args))
    }

    fn append_pairs_command(&self, cmd: &[u8], pairs: &[(&[u8], &[u8])]) -> io::Result<()> {
        self.append_encoded(encode_pairs_command(cmd, pairs))
    }
}

pub fn replay_set_commands(path: &Path, db: &ConcurrentMapDb) -> io::Result<()> {
    if !path.exists() {
        return Ok(());
    }
    let bytes = std::fs::read(path)?;
    let mut cursor = 0_usize;
    while cursor < bytes.len() {
        let (command, consumed) = parse_command(&bytes[cursor..])
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "incomplete command at end of AOF file",
                )
            })?;
        apply_replayed_command(command, db);
        cursor += consumed;
    }
    Ok(())
}

fn apply_replayed_command(command: Command<'_>, db: &ConcurrentMapDb) {
    match command {
        Command::Set(key, value, ttl_ms, condition) => apply_set_like(db, key, value, ttl_ms, condition),
        Command::SetNx(key, value) => apply_set_like(db, key, value, None, SetCondition::Nx),
        Command::SetEx(key, value, ttl_ms) | Command::PSetEx(key, value, ttl_ms) => {
            apply_set_like(db, key, value, Some(ttl_ms), SetCondition::None)
        }
        Command::GetSet(key, value) => {
            db.get_set(key, value);
        }
        Command::MSet(pairs) => {
            for (key, value) in pairs {
                db.set(key, value);
            }
        }
        Command::MSetNx(pairs) => {
            db.mset_nx(&pairs);
        }
        Command::Del(keys) => {
            for key in keys {
                db.delete(key);
            }
        }
        _ => {}
    }
}

fn encode_set_command(
    key: &[u8],
    value: &[u8],
    ttl_ms: Option<u64>,
    condition: SetCondition,
) -> Vec<u8> {
    let mut args = Vec::with_capacity(4);
    args.push(key);
    args.push(value);

    let ttl_buf = ttl_ms.map(|v| v.to_string());
    if let Some(ttl) = ttl_buf.as_ref() {
        args.push(b"PX");
        args.push(ttl.as_bytes());
    }

    match condition {
        SetCondition::None => {}
        SetCondition::Nx => args.push(b"NX"),
        SetCondition::Xx => args.push(b"XX"),
    }

    encode_command(b"SET", &args)
}

fn apply_set_like(
    db: &ConcurrentMapDb,
    key: &[u8],
    value: &[u8],
    ttl_ms: Option<u64>,
    condition: SetCondition,
) {
    match condition {
        SetCondition::None => db.set_with_optional_ttl_ms(key, value, ttl_ms),
        SetCondition::Nx => {
            db.set_nx_with_optional_ttl_ms(key, value, ttl_ms);
        }
        SetCondition::Xx => {
            db.set_xx_with_optional_ttl_ms(key, value, ttl_ms);
        }
    }
}

fn encode_command(cmd: &[u8], args: &[&[u8]]) -> Vec<u8> {
    let mut total_len = cmd.len();
    for arg in args {
        total_len += arg.len();
    }
    let mut out = Vec::with_capacity(32 + total_len);
    append_array_header(&mut out, 1 + args.len());
    append_bulk(&mut out, cmd);
    for arg in args {
        append_bulk(&mut out, arg);
    }
    out
}

fn encode_pairs_command(cmd: &[u8], pairs: &[(&[u8], &[u8])]) -> Vec<u8> {
    let mut args = Vec::with_capacity(pairs.len() * 2);
    for (key, value) in pairs {
        args.push(*key);
        args.push(*value);
    }
    encode_command(cmd, &args)
}

fn append_array_header(out: &mut Vec<u8>, len: usize) {
    out.push(b'*');
    out.extend_from_slice(len.to_string().as_bytes());
    out.extend_from_slice(b"\r\n");
}

fn append_bulk(out: &mut Vec<u8>, value: &[u8]) {
    out.push(b'$');
    out.extend_from_slice(value.len().to_string().as_bytes());
    out.extend_from_slice(b"\r\n");
    out.extend_from_slice(value);
    out.extend_from_slice(b"\r\n");
}
