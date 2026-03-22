//! RDB persistence - Redis-compatible snapshot format.
//!
//! Strategy (matches Redis):
//! - SAVE: blocking snapshot
//! - BGSAVE: background snapshot
//! - save rules: auto-trigger BGSAVE when conditions met (e.g. 900s + 1 change)

use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File};
use std::io::Cursor;
use std::io::{self, BufReader, Read, Write};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use crc64::crc64;

use crate::command::shared::typed_value::{TypedValue, decode_value};
use crate::command::shared::zset::ZSet;
use crate::config::model::{RdbConfig, SaveRule};
use crate::storage::StorageEngine;

const RDB_VERSION: &[u8; 4] = b"0009";
const RDB_MAGIC: &[u8; 5] = b"REDIS";

// Opcodes
const RDB_OPCODE_EOF: u8 = 0xFF;
const RDB_OPCODE_SELECTDB: u8 = 0xFE;
const RDB_OPCODE_RESIZEDB: u8 = 0xFB;
const RDB_OPCODE_EXPIRETIME_MS: u8 = 0xFC;
const RDB_OPCODE_AUX: u8 = 0xFA;

// Value types (Redis RDB)
const RDB_TYPE_STRING: u8 = 0;
const RDB_TYPE_LIST: u8 = 1;
const RDB_TYPE_SET: u8 = 2;
const RDB_TYPE_ZSET: u8 = 3;
const RDB_TYPE_HASH: u8 = 4;

#[derive(Debug, Clone)]
pub struct RdbRuntimeConfig {
    pub file_path: PathBuf,
    pub save_rules: Vec<SaveRule>,
    pub rdbchecksum: bool,
}

impl RdbRuntimeConfig {
    pub fn from_config(cfg: &RdbConfig) -> Self {
        Self {
            file_path: cfg.dir.join(&cfg.dbfilename),
            save_rules: cfg.save_rules.clone(),
            rdbchecksum: cfg.rdbchecksum,
        }
    }
}

/// Tracks dirty count and last save time for save rules.
#[derive(Default)]
pub struct RdbDirtyTracker {
    dirty: AtomicU64,
    last_save_time: AtomicU64,
}

impl RdbDirtyTracker {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn incr_dirty(&self) {
        self.dirty.fetch_add(1, Ordering::Relaxed);
    }

    pub fn reset_dirty(&self) {
        self.dirty.store(0, Ordering::Relaxed);
    }

    pub fn set_last_save(&self, unix_secs: u64) {
        self.last_save_time.store(unix_secs, Ordering::Relaxed);
    }

    pub fn dirty(&self) -> u64 {
        self.dirty.load(Ordering::Relaxed)
    }

    pub fn last_save_time(&self) -> u64 {
        self.last_save_time.load(Ordering::Relaxed)
    }
}

/// Save database to RDB file. Blocking.
pub fn save(
    db: &dyn StorageEngine,
    config: &RdbRuntimeConfig,
    dirty_tracker: Option<&RdbDirtyTracker>,
) -> io::Result<()> {
    let temp_path = config.file_path.with_extension("tmp");
    if let Some(parent) = temp_path.parent() {
        fs::create_dir_all(parent)?;
    }

    let mut buf = Vec::new();
    write_rdb(db, &mut buf)?;

    if config.rdbchecksum {
        let checksum = crc64(0, &buf);
        buf.extend_from_slice(&checksum.to_le_bytes());
    }

    let mut file = File::create(&temp_path)?;
    file.write_all(&buf)?;
    file.flush()?;
    drop(file);
    fs::rename(temp_path, &config.file_path)?;

    if let Some(tracker) = dirty_tracker {
        tracker.reset_dirty();
        tracker.set_last_save(now_secs());
    }

    Ok(())
}

/// Build an in-memory RDB snapshot payload for replication FULLRESYNC.
/// Returns the raw RDB bytes without RESP framing.
pub fn build_rdb_snapshot_bytes(
    db: &dyn StorageEngine,
    with_checksum: bool,
) -> io::Result<Vec<u8>> {
    let mut buf = Vec::new();
    write_rdb(db, &mut buf)?;
    if with_checksum {
        let checksum = crc64(0, &buf);
        buf.extend_from_slice(&checksum.to_le_bytes());
    }
    Ok(buf)
}

/// Load database from RDB file. Returns number of keys loaded.
pub fn load(db: &dyn StorageEngine, path: &PathBuf) -> io::Result<usize> {
    if !path.exists() {
        return Ok(0);
    }

    let file = File::open(path)?;
    let mut r = BufReader::new(file);
    load_from_reader(db, &mut r)
}

/// Load database from in-memory RDB bytes. Returns number of keys loaded.
pub fn load_from_bytes(db: &dyn StorageEngine, bytes: &[u8]) -> io::Result<usize> {
    let mut cursor = Cursor::new(bytes);
    load_from_reader(db, &mut cursor)
}

fn load_from_reader(db: &dyn StorageEngine, r: &mut impl Read) -> io::Result<usize> {
    let mut magic = [0u8; 5];
    r.read_exact(&mut magic)?;
    if &magic != RDB_MAGIC {
        return Err(io::Error::other("invalid RDB file: bad magic"));
    }

    let mut version = [0u8; 4];
    r.read_exact(&mut version)?;
    let _version = std::str::from_utf8(&version).unwrap_or("0000");

    let mut keys_loaded = 0_usize;

    loop {
        let opcode = read_byte(r)?;
        match opcode {
            RDB_OPCODE_AUX => {
                let _key = read_rdb_string(r)?;
                let _value = read_rdb_string(r)?;
            }
            RDB_OPCODE_SELECTDB => {
                let _db_number = read_length(r)?;
            }
            RDB_OPCODE_RESIZEDB => {
                let _hash_size = read_length(r)?;
                let _expire_size = read_length(r)?;
            }
            RDB_OPCODE_EXPIRETIME_MS => {
                let mut ts_bytes = [0u8; 8];
                r.read_exact(&mut ts_bytes)?;
                let expire_at_ms = u64::from_le_bytes(ts_bytes);
                let value_type = read_byte(r)?;
                let key = read_rdb_string(r)?;
                let value = read_rdb_value(r, value_type)?;
                db.put_entry(
                    &key,
                    crate::storage::ValueEntry {
                        value: Bytes::from(value),
                        expire_at_ms: Some(expire_at_ms),
                    },
                );
                keys_loaded += 1;
            }
            RDB_OPCODE_EOF => break,
            _ => {
                if opcode == RDB_TYPE_STRING
                    || opcode == RDB_TYPE_LIST
                    || opcode == RDB_TYPE_SET
                    || opcode == RDB_TYPE_ZSET
                    || opcode == RDB_TYPE_HASH
                {
                    let key = read_rdb_string(r)?;
                    let value = read_rdb_value(r, opcode)?;
                    db.put_entry(
                        &key,
                        crate::storage::ValueEntry {
                            value: Bytes::from(value),
                            expire_at_ms: None,
                        },
                    );
                    keys_loaded += 1;
                } else {
                    return Err(io::Error::other(format!(
                        "unknown RDB opcode: 0x{:02x}",
                        opcode
                    )));
                }
            }
        }
    }

    // Read 8-byte checksum if present (rdbchecksum yes)
    let mut crc_bytes = [0u8; 8];
    let _ = r.read_exact(&mut crc_bytes);

    Ok(keys_loaded)
}

/// Background save. Spawns a thread. Returns immediately.
/// `bgsave_in_progress` is set to true while saving.
pub fn bgsave(
    db: Arc<dyn StorageEngine + Send + Sync>,
    config: RdbRuntimeConfig,
    dirty_tracker: Option<Arc<RdbDirtyTracker>>,
    bgsave_in_progress: Arc<AtomicBool>,
) {
    if bgsave_in_progress.swap(true, Ordering::SeqCst) {
        return;
    }

    std::thread::spawn(move || {
        let result = save(db.as_ref(), &config, dirty_tracker.as_deref());
        bgsave_in_progress.store(false, Ordering::SeqCst);
        if let Err(e) = result {
            eprintln!("BGSAVE failed: {e}");
        }
    });
}

/// Check save rules and trigger BGSAVE if conditions met.
pub fn maybe_trigger_bgsave(
    db: Arc<dyn StorageEngine + Send + Sync>,
    config: RdbRuntimeConfig,
    dirty_tracker: Arc<RdbDirtyTracker>,
    bgsave_in_progress: Arc<AtomicBool>,
) {
    let now = now_secs();
    let dirty = dirty_tracker.dirty();
    if dirty == 0 {
        return;
    }

    for rule in &config.save_rules {
        let elapsed = now.saturating_sub(dirty_tracker.last_save_time());
        if elapsed >= rule.seconds && dirty >= rule.changes {
            bgsave(db, config, Some(dirty_tracker), bgsave_in_progress);
            break;
        }
    }
}

fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

fn write_rdb(db: &dyn StorageEngine, w: &mut dyn Write) -> io::Result<()> {
    w.write_all(RDB_MAGIC)?;
    w.write_all(RDB_VERSION)?;

    // AUX: redis-ver
    w.write_all(&[RDB_OPCODE_AUX])?;
    write_rdb_string(w, b"redis-ver")?;
    write_rdb_string(w, env!("CARGO_PKG_VERSION").as_bytes())?;

    // AUX: redis-bits
    w.write_all(&[RDB_OPCODE_AUX])?;
    write_rdb_string(w, b"redis-bits")?;
    write_rdb_string(w, b"64")?;

    // SELECTDB 0
    w.write_all(&[RDB_OPCODE_SELECTDB])?;
    write_length(w, 0)?;

    let keys = db.iter_live_keys();
    let expire_count = 0_usize;

    // RESIZEDB
    w.write_all(&[RDB_OPCODE_RESIZEDB])?;
    write_length(w, keys.len())?;
    write_length(w, expire_count)?;

    let now_ms = now_ms();
    for key in keys {
        let Some(entry) = db.get_entry(&key) else {
            continue;
        };

        if let Some(expire_at_ms) = entry.expire_at_ms {
            if expire_at_ms <= now_ms {
                continue;
            }
            w.write_all(&[RDB_OPCODE_EXPIRETIME_MS])?;
            w.write_all(&expire_at_ms.to_le_bytes())?;
        }

        let (value_type, value_bytes) = encode_value_for_rdb(&entry.value)?;
        w.write_all(&[value_type])?;
        write_rdb_string(w, &key)?;
        write_rdb_string(w, &value_bytes)?;
    }

    w.write_all(&[RDB_OPCODE_EOF])?;
    Ok(())
}

fn encode_value_for_rdb(raw: &[u8]) -> io::Result<(u8, Vec<u8>)> {
    let typed = decode_value(raw).map_err(io::Error::other)?;
    match typed {
        TypedValue::String(s) => Ok((RDB_TYPE_STRING, s)),
        TypedValue::List(items) => {
            let mut out = Vec::new();
            write_length(&mut out, items.len())?;
            for item in items {
                write_rdb_string(&mut out, item.as_slice())?;
            }
            Ok((RDB_TYPE_LIST, out))
        }
        TypedValue::Set(set) => {
            let mut out = Vec::new();
            write_length(&mut out, set.len())?;
            for member in set {
                write_rdb_string(&mut out, member.as_slice())?;
            }
            Ok((RDB_TYPE_SET, out))
        }
        TypedValue::ZSet(zset) => {
            let mut out = Vec::new();
            write_length(&mut out, zset.len())?;
            for (member, score) in zset.iter() {
                write_rdb_string(&mut out, member.as_slice())?;
                let score_str = score.to_string();
                write_rdb_string(&mut out, score_str.as_bytes())?;
            }
            Ok((RDB_TYPE_ZSET, out))
        }
        TypedValue::Hash(map) => {
            let mut out = Vec::new();
            write_length(&mut out, map.len())?;
            for (k, v) in map {
                write_rdb_string(&mut out, k.as_slice())?;
                write_rdb_string(&mut out, v.as_slice())?;
            }
            Ok((RDB_TYPE_HASH, out))
        }
    }
}

fn write_length(w: &mut dyn Write, len: usize) -> io::Result<()> {
    if len < 64 {
        w.write_all(&[len as u8])?;
    } else if len < 16384 {
        let b0 = 0x40 | ((len >> 8) as u8);
        let b1 = (len & 0xFF) as u8;
        w.write_all(&[b0, b1])?;
    } else {
        w.write_all(&[0x80])?;
        w.write_all(&(len as u32).to_le_bytes())?;
    }
    Ok(())
}

fn write_rdb_string(w: &mut dyn Write, s: &[u8]) -> io::Result<()> {
    write_length(w, s.len())?;
    w.write_all(s)?;
    Ok(())
}

fn read_byte(r: &mut impl Read) -> io::Result<u8> {
    let mut buf = [0u8; 1];
    r.read_exact(&mut buf)?;
    Ok(buf[0])
}

fn read_length(r: &mut impl Read) -> io::Result<usize> {
    let b0 = read_byte(r)?;
    let high2 = b0 >> 6;
    match high2 {
        0 => Ok((b0 & 0x3F) as usize),
        1 => {
            let b1 = read_byte(r)?;
            Ok(((b0 & 0x3F) as usize) << 8 | (b1 as usize))
        }
        2 => {
            let mut buf = [0u8; 4];
            r.read_exact(&mut buf)?;
            Ok(u32::from_le_bytes(buf) as usize)
        }
        _ => Err(io::Error::other("invalid length encoding")),
    }
}

fn read_rdb_string(r: &mut impl Read) -> io::Result<Vec<u8>> {
    let len = read_length(r)?;
    let mut buf = vec![0u8; len];
    r.read_exact(&mut buf)?;
    Ok(buf)
}

fn read_rdb_value(r: &mut impl Read, value_type: u8) -> io::Result<Vec<u8>> {
    match value_type {
        RDB_TYPE_STRING => {
            let s = read_rdb_string(r)?;
            Ok(s)
        }
        RDB_TYPE_LIST => {
            let len = read_length(r)?;
            let mut items = Vec::with_capacity(len);
            for _ in 0..len {
                items.push(read_rdb_string(r)?);
            }
            Ok(encode_list(&items))
        }
        RDB_TYPE_SET => {
            let len = read_length(r)?;
            let mut set = BTreeSet::new();
            for _ in 0..len {
                set.insert(read_rdb_string(r)?);
            }
            Ok(encode_set(&set))
        }
        RDB_TYPE_ZSET => {
            let len = read_length(r)?;
            let mut zset = ZSet::new();
            for _ in 0..len {
                let member = read_rdb_string(r)?;
                let score_str = read_rdb_string(r)?;
                let score = std::str::from_utf8(&score_str)
                    .ok()
                    .and_then(|s| s.parse::<f64>().ok())
                    .unwrap_or(0.0);
                let _ = zset.add(member, score);
            }
            Ok(encode_zset(&zset))
        }
        RDB_TYPE_HASH => {
            let len = read_length(r)?;
            let mut map = BTreeMap::new();
            for _ in 0..len {
                let k = read_rdb_string(r)?;
                let v = read_rdb_string(r)?;
                map.insert(k, v);
            }
            Ok(encode_hash(&map))
        }
        _ => Err(io::Error::other(format!(
            "unknown value type: {}",
            value_type
        ))),
    }
}

fn encode_list(items: &[Vec<u8>]) -> Vec<u8> {
    use crate::command::shared::typed_value::encode_list;
    encode_list(items)
}

fn encode_set(set: &BTreeSet<Vec<u8>>) -> Vec<u8> {
    use crate::command::shared::typed_value::encode_set;
    encode_set(set)
}

fn encode_zset(zset: &ZSet) -> Vec<u8> {
    use crate::command::shared::typed_value::encode_zset;
    encode_zset(zset)
}

fn encode_hash(map: &BTreeMap<Vec<u8>, Vec<u8>>) -> Vec<u8> {
    use crate::command::shared::typed_value::encode_hash;
    encode_hash(map)
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::Path;
    use std::path::PathBuf;
    use std::sync::Arc;
    use std::sync::atomic::Ordering;
    use std::thread;
    use std::time::Duration;

    use bytes::Bytes;

    use super::*;
    use crate::config::model::{RdbConfig, SaveRule};
    use crate::storage::{DashMapStorageEngine, DbConfig, StorageEngine, ValueEntry};

    fn temp_rdb_path(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!("foxkv_rdb_test_{}.rdb", name))
    }

    fn cleanup(path: &PathBuf) {
        let _ = fs::remove_file(path);
    }

    fn test_db() -> Arc<dyn StorageEngine + Send + Sync> {
        Arc::new(DashMapStorageEngine::new(DbConfig { worker_count: 2 }).expect("db init failed"))
    }

    fn create_test_config(path: &Path) -> RdbRuntimeConfig {
        RdbRuntimeConfig {
            file_path: path.to_path_buf(),
            save_rules: vec![SaveRule {
                seconds: 900,
                changes: 1,
            }],
            rdbchecksum: true,
        }
    }

    #[test]
    fn rdb_runtime_config_from_config_converts_correctly() {
        let cfg = RdbConfig {
            save_rules: vec![SaveRule {
                seconds: 60,
                changes: 10,
            }],
            dbfilename: "dump.rdb".to_string(),
            dir: PathBuf::from("/data"),
            stop_writes_on_bgsave_error: true,
            rdbcompression: true,
            rdbchecksum: false,
            rdb_save_incremental_fsync: true,
        };
        let runtime = RdbRuntimeConfig::from_config(&cfg);
        assert_eq!(runtime.file_path, PathBuf::from("/data/dump.rdb"));
        assert_eq!(runtime.save_rules.len(), 1);
        assert_eq!(runtime.save_rules[0].seconds, 60);
        assert_eq!(runtime.save_rules[0].changes, 10);
        assert!(!runtime.rdbchecksum);
    }

    #[test]
    fn rdb_dirty_tracker_incr_dirty_increments_counter() {
        let tracker = RdbDirtyTracker::new();
        assert_eq!(tracker.dirty(), 0);
        tracker.incr_dirty();
        assert_eq!(tracker.dirty(), 1);
        tracker.incr_dirty();
        tracker.incr_dirty();
        assert_eq!(tracker.dirty(), 3);
    }

    #[test]
    fn rdb_dirty_tracker_reset_dirty_sets_counter_to_zero() {
        let tracker = RdbDirtyTracker::new();
        tracker.incr_dirty();
        tracker.incr_dirty();
        assert_eq!(tracker.dirty(), 2);
        tracker.reset_dirty();
        assert_eq!(tracker.dirty(), 0);
    }

    #[test]
    fn rdb_dirty_tracker_set_last_save_updates_timestamp() {
        let tracker = RdbDirtyTracker::new();
        tracker.set_last_save(1700000000);
        assert_eq!(tracker.last_save_time(), 1700000000);
        tracker.set_last_save(1800000000);
        assert_eq!(tracker.last_save_time(), 1800000000);
    }

    #[test]
    fn save_creates_file_with_valid_rdb_format() {
        let path = temp_rdb_path("save_creates");
        cleanup(&path);
        let db = test_db();
        db.put_entry(
            b"key",
            ValueEntry {
                value: Bytes::from("value"),
                expire_at_ms: None,
            },
        );
        let config = create_test_config(&path);
        save(db.as_ref(), &config, None).unwrap();
        assert!(path.exists());
        let contents = fs::read(&path).unwrap();
        assert!(contents.starts_with(b"REDIS"));
        cleanup(&path);
    }

    #[test]
    fn save_with_dirty_tracker_resets_dirty_and_updates_last_save() {
        let path = temp_rdb_path("save_tracker");
        cleanup(&path);
        let db = test_db();
        db.put_entry(
            b"key",
            ValueEntry {
                value: Bytes::from("value"),
                expire_at_ms: None,
            },
        );
        let tracker = RdbDirtyTracker::new();
        tracker.incr_dirty();
        tracker.incr_dirty();
        tracker.set_last_save(0);
        let config = create_test_config(&path);
        save(db.as_ref(), &config, Some(&tracker)).unwrap();
        assert_eq!(tracker.dirty(), 0);
        assert!(tracker.last_save_time() > 0);
        cleanup(&path);
    }

    #[test]
    fn load_returns_zero_for_nonexistent_file() {
        let path = temp_rdb_path("nonexistent");
        cleanup(&path);
        let db = test_db();
        let count = load(db.as_ref(), &path).unwrap();
        assert_eq!(count, 0);
    }

    #[test]
    fn load_from_bytes_returns_error_for_invalid_magic() {
        let db = test_db();
        let result = load_from_bytes(db.as_ref(), b"INVALID");
        assert!(result.is_err());
    }

    #[test]
    fn load_from_bytes_loads_data_from_valid_rdb() {
        let db1 = test_db();
        db1.put_entry(
            b"key",
            ValueEntry {
                value: Bytes::from("value"),
                expire_at_ms: None,
            },
        );
        let bytes = build_rdb_snapshot_bytes(db1.as_ref(), true).unwrap();
        let db2 = test_db();
        let count = load_from_bytes(db2.as_ref(), &bytes).unwrap();
        assert_eq!(count, 1);
        let entry = db2.get_entry(b"key").expect("key should exist");
        assert_eq!(entry.value.as_ref(), b"value");
    }

    #[test]
    fn save_and_load_roundtrip_preserves_string_data() {
        let path = temp_rdb_path("roundtrip_string");
        cleanup(&path);
        let db = test_db();
        db.put_entry(
            b"mykey",
            ValueEntry {
                value: Bytes::from("myvalue"),
                expire_at_ms: None,
            },
        );
        let config = create_test_config(&path);
        save(db.as_ref(), &config, None).unwrap();
        let db2 = test_db();
        let count = load(db2.as_ref(), &path).unwrap();
        assert_eq!(count, 1);
        let entry = db2.get_entry(b"mykey").expect("key should exist");
        assert_eq!(entry.value.as_ref(), b"myvalue");
        cleanup(&path);
    }

    #[test]
    fn save_and_load_roundtrip_preserves_expire_time() {
        let path = temp_rdb_path("roundtrip_expire");
        cleanup(&path);
        let db = test_db();
        let future_expire = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64
            + 3600000;
        db.put_entry(
            b"expkey",
            ValueEntry {
                value: Bytes::from("expvalue"),
                expire_at_ms: Some(future_expire),
            },
        );
        let config = create_test_config(&path);
        save(db.as_ref(), &config, None).unwrap();
        let db2 = test_db();
        let count = load(db2.as_ref(), &path).unwrap();
        assert_eq!(count, 1);
        let entry = db2.get_entry(b"expkey").expect("key should exist");
        assert_eq!(entry.value.as_ref(), b"expvalue");
        assert!(entry.expire_at_ms.is_some());
        cleanup(&path);
    }

    #[test]
    fn save_and_load_roundtrip_multiple_keys() {
        let path = temp_rdb_path("roundtrip_multi");
        cleanup(&path);
        let db = test_db();
        for i in 0..10 {
            let key = format!("key{}", i);
            let value = format!("value{}", i);
            db.put_entry(
                key.as_bytes(),
                ValueEntry {
                    value: Bytes::from(value),
                    expire_at_ms: None,
                },
            );
        }
        let config = create_test_config(&path);
        save(db.as_ref(), &config, None).unwrap();
        let db2 = test_db();
        let count = load(db2.as_ref(), &path).unwrap();
        assert_eq!(count, 10);
        for i in 0..10 {
            let key = format!("key{}", i);
            let value = format!("value{}", i);
            let entry = db2.get_entry(key.as_bytes()).expect("key should exist");
            assert_eq!(entry.value.as_ref(), value.as_bytes());
        }
        cleanup(&path);
    }

    #[test]
    fn build_rdb_snapshot_bytes_produces_valid_rdb() {
        let db = test_db();
        db.put_entry(
            b"key",
            ValueEntry {
                value: Bytes::from("value"),
                expire_at_ms: None,
            },
        );
        let bytes = build_rdb_snapshot_bytes(db.as_ref(), true).unwrap();
        assert!(bytes.starts_with(b"REDIS"));
        let checksum_bytes = &bytes[bytes.len() - 8..];
        let stored_checksum = u64::from_le_bytes([
            checksum_bytes[0],
            checksum_bytes[1],
            checksum_bytes[2],
            checksum_bytes[3],
            checksum_bytes[4],
            checksum_bytes[5],
            checksum_bytes[6],
            checksum_bytes[7],
        ]);
        let computed_checksum = crc64(0, &bytes[..bytes.len() - 8]);
        assert_eq!(stored_checksum, computed_checksum);
    }

    #[test]
    fn build_rdb_snapshot_bytes_without_checksum() {
        let db = test_db();
        db.put_entry(
            b"key",
            ValueEntry {
                value: Bytes::from("value"),
                expire_at_ms: None,
            },
        );
        let bytes = build_rdb_snapshot_bytes(db.as_ref(), false).unwrap();
        assert!(bytes.starts_with(b"REDIS"));
        let computed_checksum = crc64(0, &bytes);
        assert_ne!(computed_checksum, 0);
    }

    #[test]
    fn bgsave_runs_in_background_and_updates_file() {
        let path = temp_rdb_path("bgsave");
        cleanup(&path);
        let db = test_db();
        db.put_entry(
            b"key",
            ValueEntry {
                value: Bytes::from("value"),
                expire_at_ms: None,
            },
        );
        let config = create_test_config(&path);
        let bgsave_in_progress = Arc::new(AtomicBool::new(false));
        bgsave(db.clone(), config, None, bgsave_in_progress.clone());
        thread::sleep(Duration::from_millis(100));
        let mut attempts = 0;
        while bgsave_in_progress.load(Ordering::SeqCst) && attempts < 50 {
            thread::sleep(Duration::from_millis(50));
            attempts += 1;
        }
        assert!(!bgsave_in_progress.load(Ordering::SeqCst));
        assert!(path.exists());
        cleanup(&path);
    }

    #[test]
    fn bgsave_skips_if_already_in_progress() {
        let path = temp_rdb_path("bgsave_skip");
        cleanup(&path);
        let db = test_db();
        let config = create_test_config(&path);
        let bgsave_in_progress = Arc::new(AtomicBool::new(true));
        bgsave(db.clone(), config, None, bgsave_in_progress.clone());
        thread::sleep(Duration::from_millis(50));
        assert!(bgsave_in_progress.load(Ordering::SeqCst));
        bgsave_in_progress.store(false, Ordering::SeqCst);
        cleanup(&path);
    }

    #[test]
    fn maybe_trigger_bgsave_triggers_when_rule_matches() {
        let path = temp_rdb_path("maybe_trigger");
        cleanup(&path);
        let db = test_db();
        db.put_entry(
            b"key",
            ValueEntry {
                value: Bytes::from("value"),
                expire_at_ms: None,
            },
        );
        let config = RdbRuntimeConfig {
            file_path: path.clone(),
            save_rules: vec![SaveRule {
                seconds: 0,
                changes: 1,
            }],
            rdbchecksum: true,
        };
        let tracker = Arc::new(RdbDirtyTracker::new());
        tracker.incr_dirty();
        tracker.set_last_save(0);
        let bgsave_in_progress = Arc::new(AtomicBool::new(false));
        maybe_trigger_bgsave(
            db.clone(),
            config,
            tracker.clone(),
            bgsave_in_progress.clone(),
        );
        thread::sleep(Duration::from_millis(100));
        let mut attempts = 0;
        while bgsave_in_progress.load(Ordering::SeqCst) && attempts < 50 {
            thread::sleep(Duration::from_millis(50));
            attempts += 1;
        }
        assert!(path.exists());
        cleanup(&path);
    }

    #[test]
    fn maybe_trigger_bgsave_skips_when_dirty_is_zero() {
        let path = temp_rdb_path("maybe_skip_zero");
        cleanup(&path);
        let db = test_db();
        let config = create_test_config(&path);
        let tracker = Arc::new(RdbDirtyTracker::new());
        let bgsave_in_progress = Arc::new(AtomicBool::new(false));
        maybe_trigger_bgsave(
            db.clone(),
            config,
            tracker.clone(),
            bgsave_in_progress.clone(),
        );
        thread::sleep(Duration::from_millis(50));
        assert!(!path.exists());
        cleanup(&path);
    }

    #[test]
    fn save_creates_parent_directory_if_not_exists() {
        let temp_dir = std::env::temp_dir().join("foxkv_test_subdir");
        let _ = fs::remove_dir_all(&temp_dir);
        let path = temp_dir.join("test.rdb");
        let db = test_db();
        db.put_entry(
            b"key",
            ValueEntry {
                value: Bytes::from("value"),
                expire_at_ms: None,
            },
        );
        let config = RdbRuntimeConfig {
            file_path: path.clone(),
            save_rules: vec![],
            rdbchecksum: false,
        };
        save(db.as_ref(), &config, None).unwrap();
        assert!(path.exists());
        let _ = fs::remove_dir_all(&temp_dir);
    }

    #[test]
    fn save_without_checksum_omits_checksum_bytes() {
        let path = temp_rdb_path("no_checksum");
        cleanup(&path);
        let db = test_db();
        db.put_entry(
            b"key",
            ValueEntry {
                value: Bytes::from("value"),
                expire_at_ms: None,
            },
        );
        let config = RdbRuntimeConfig {
            file_path: path.clone(),
            save_rules: vec![],
            rdbchecksum: false,
        };
        save(db.as_ref(), &config, None).unwrap();
        let contents = fs::read(&path).unwrap();
        assert!(contents.starts_with(b"REDIS"));
        assert!(contents.windows(1).any(|w| w[0] == RDB_OPCODE_EOF));
        cleanup(&path);
    }
}
