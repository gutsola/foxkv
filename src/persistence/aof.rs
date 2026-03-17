use std::fs::{File, OpenOptions};
use std::io;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use crate::command::{Command, parse_command};
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

    pub fn append_set(&self, key: &[u8], value: &[u8], ttl_ms: Option<u64>) -> io::Result<()> {
        let mut payload = encode_set_command(key, value, ttl_ms);
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
        payload.clear();
        Ok(())
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
    if let Command::Set(key, value, ttl_ms) = command {
        if let Some(ttl_ms) = ttl_ms {
            db.set_with_ttl_ms(key, value, ttl_ms);
        } else {
            db.set(key, value);
        }
    }
}

fn encode_set_command(key: &[u8], value: &[u8], ttl_ms: Option<u64>) -> Vec<u8> {
    let mut out = Vec::with_capacity(64 + key.len() + value.len());
    let array_len = if ttl_ms.is_some() { 5 } else { 3 };
    append_array_header(&mut out, array_len);
    append_bulk(&mut out, b"SET");
    append_bulk(&mut out, key);
    append_bulk(&mut out, value);
    if let Some(ttl_ms) = ttl_ms {
        append_bulk(&mut out, b"PX");
        append_bulk(&mut out, ttl_ms.to_string().as_bytes());
    }
    out
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
