use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use ahash::RandomState;
use dashmap::mapref::entry::Entry as DashEntry;
use dashmap::DashMap;

use crate::storage::db::StorageEngine;
use crate::storage::model::{DbConfig, DbError, ValueEntry};

#[derive(Clone)]
pub struct DashMapStorageEngine {
    inner: Arc<DashMap<Vec<u8>, ValueEntry, RandomState>>,
}

impl DashMapStorageEngine {
    pub fn new(config: DbConfig) -> Result<Self, DbError> {
        validate_db_config(config)?;
        let shard_amount = config.worker_count.max(2).next_power_of_two();
        let map = DashMap::with_hasher_and_shard_amount(RandomState::new(), shard_amount);
        Ok(Self {
            inner: Arc::new(map),
        })
    }

    fn exists_at(&self, key: &[u8], now: u64) -> bool {
        if let Some(entry) = self.inner.get(key) {
            if is_expired(&entry, now) {
                drop(entry);
                self.inner.remove(key);
                return false;
            }
            return true;
        }
        false
    }

    fn collect_live_keys(&self) -> Vec<Vec<u8>> {
        let now = now_ms();
        let mut expired = Vec::new();
        let mut keys = Vec::new();

        for entry in self.inner.iter() {
            if is_expired(entry.value(), now) {
                expired.push(entry.key().clone());
            } else {
                keys.push(entry.key().clone());
            }
        }
        for key in expired {
            self.inner.remove(&key);
        }
        keys
    }
}

impl StorageEngine for DashMapStorageEngine {
    fn get_entry(&self, key: &[u8]) -> Option<ValueEntry> {
        let now = now_ms();
        if let Some(entry) = self.inner.get(key) {
            if is_expired(&entry, now) {
                drop(entry);
                self.inner.remove(key);
                return None;
            }
            return Some(ValueEntry {
                value: entry.value.clone(),
                expire_at_ms: entry.expire_at_ms,
            });
        }
        None
    }

    fn put_entry(&self, key: &[u8], entry: ValueEntry) {
        self.inner.insert(key.to_vec(), entry);
    }

    fn put_if_absent(&self, key: &[u8], entry: ValueEntry) -> bool {
        let now = now_ms();
        if let Some(mut occupied) = self.inner.get_mut(key) {
            if is_expired(&occupied, now) {
                *occupied = entry;
                return true;
            }
            return false;
        }

        match self.inner.entry(key.to_vec()) {
            DashEntry::Vacant(vacant) => {
                vacant.insert(entry);
                true
            }
            DashEntry::Occupied(mut occupied) => {
                if is_expired(occupied.get(), now) {
                    occupied.insert(entry);
                    true
                } else {
                    false
                }
            }
        }
    }

    fn put_if_present(&self, key: &[u8], entry: ValueEntry) -> bool {
        let now = now_ms();
        let Some(mut occupied) = self.inner.get_mut(key) else {
            return false;
        };
        if is_expired(&occupied, now) {
            drop(occupied);
            self.inner.remove(key);
            return false;
        }
        *occupied = entry;
        true
    }

    fn remove_entry(&self, key: &[u8]) -> bool {
        self.inner.remove(key).is_some()
    }

    fn contains_live_key(&self, key: &[u8]) -> bool {
        self.exists_at(key, now_ms())
    }

    fn iter_live_keys(&self) -> Vec<Vec<u8>> {
        self.collect_live_keys()
    }

    fn scan_live_keys(&self, cursor: usize, count: usize) -> (usize, Vec<Vec<u8>>) {
        let mut keys = self.collect_live_keys();
        keys.sort();

        let start = cursor.min(keys.len());
        let end = start.saturating_add(count).min(keys.len());
        let next_cursor = if end >= keys.len() { 0 } else { end };
        let items = keys[start..end].to_vec();
        (next_cursor, items)
    }

    fn flush_all(&self) {
        self.inner.clear();
    }
}

fn validate_db_config(config: DbConfig) -> Result<(), DbError> {
    if config.worker_count == 0 {
        return Err(DbError::InvalidConfig("worker_count must be > 0"));
    }
    Ok(())
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

fn is_expired(entry: &ValueEntry, now: u64) -> bool {
    entry
        .expire_at_ms
        .is_some_and(|expire_at_ms| expire_at_ms <= now)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::thread;
    use std::time::{SystemTime, UNIX_EPOCH};

    use bytes::Bytes;

    use super::*;
    use crate::storage::db::StorageEngine;

    fn engine() -> DashMapStorageEngine {
        DashMapStorageEngine::new(DbConfig { worker_count: 4 }).expect("engine should initialize")
    }

    fn entry(value: &str, expire_at_ms: Option<u64>) -> ValueEntry {
        ValueEntry {
            value: Bytes::from(value.to_owned()),
            expire_at_ms,
        }
    }

    fn now_ms_for_test() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64
    }

    #[test]
    fn get_returns_none_when_key_does_not_exist() {
        let db = engine();
        assert!(db.get_entry(b"missing").is_none());
    }

    #[test]
    fn put_entry_and_get_entry_returns_written_value() {
        let db = engine();
        db.put_entry(b"k1", entry("v1", None));

        let got = db.get_entry(b"k1").expect("key should exist");
        assert_eq!(got.value, Bytes::from("v1"));
        assert_eq!(got.expire_at_ms, None);
    }

    #[test]
    fn get_returns_none_when_key_is_expired() {
        let db = engine();
        let expired = now_ms_for_test().saturating_sub(1);
        db.put_entry(b"expired", entry("v", Some(expired)));

        assert!(db.get_entry(b"expired").is_none());
        assert!(!db.contains_live_key(b"expired"));
    }

    #[test]
    fn put_if_absent_returns_true_when_key_does_not_exist() {
        let db = engine();
        assert!(db.put_if_absent(b"k1", entry("v1", None)));

        let got = db.get_entry(b"k1").expect("key should be inserted");
        assert_eq!(got.value, Bytes::from("v1"));
    }

    #[test]
    fn put_if_absent_returns_false_when_key_exists_and_not_expired() {
        let db = engine();
        db.put_entry(b"k1", entry("old", None));

        assert!(!db.put_if_absent(b"k1", entry("new", None)));
        let got = db.get_entry(b"k1").expect("key should remain");
        assert_eq!(got.value, Bytes::from("old"));
    }

    #[test]
    fn put_if_absent_overwrites_when_existing_key_is_expired() {
        let db = engine();
        let expired = now_ms_for_test().saturating_sub(1);
        db.put_entry(b"k1", entry("old", Some(expired)));

        assert!(db.put_if_absent(b"k1", entry("new", None)));
        let got = db.get_entry(b"k1").expect("key should be replaced");
        assert_eq!(got.value, Bytes::from("new"));
    }

    #[test]
    fn put_if_present_returns_false_when_key_does_not_exist() {
        let db = engine();
        assert!(!db.put_if_present(b"missing", entry("v", None)));
    }

    #[test]
    fn put_if_present_returns_false_when_existing_key_is_expired() {
        let db = engine();
        let expired = now_ms_for_test().saturating_sub(1);
        db.put_entry(b"k1", entry("old", Some(expired)));

        assert!(!db.put_if_present(b"k1", entry("new", None)));
        assert!(!db.contains_live_key(b"k1"));
    }

    #[test]
    fn put_if_present_updates_value_when_key_exists() {
        let db = engine();
        db.put_entry(b"k1", entry("old", None));

        assert!(db.put_if_present(b"k1", entry("new", None)));
        let got = db.get_entry(b"k1").expect("key should be updated");
        assert_eq!(got.value, Bytes::from("new"));
    }

    #[test]
    fn remove_entry_returns_expected_flags() {
        let db = engine();
        db.put_entry(b"k1", entry("v", None));

        assert!(db.remove_entry(b"k1"));
        assert!(!db.remove_entry(b"k1"));
    }

    #[test]
    fn contains_live_key_returns_false_when_key_is_expired() {
        let db = engine();
        let expired = now_ms_for_test().saturating_sub(1);
        db.put_entry(b"k1", entry("v", Some(expired)));

        assert!(!db.contains_live_key(b"k1"));
    }

    #[test]
    fn iter_live_keys_excludes_expired_keys() {
        let db = engine();
        let expired = now_ms_for_test().saturating_sub(1);
        db.put_entry(b"a", entry("1", None));
        db.put_entry(b"b", entry("2", Some(expired)));
        db.put_entry(b"c", entry("3", None));

        let mut keys = db.iter_live_keys();
        keys.sort();
        assert_eq!(keys, vec![b"a".to_vec(), b"c".to_vec()]);
        assert!(!db.contains_live_key(b"b"));
    }

    #[test]
    fn scan_live_keys_returns_sorted_pages_when_count_is_limited() {
        let db = engine();
        db.put_entry(b"b", entry("2", None));
        db.put_entry(b"a", entry("1", None));
        db.put_entry(b"c", entry("3", None));

        let (next, first_page) = db.scan_live_keys(0, 2);
        assert_eq!(next, 2);
        assert_eq!(first_page, vec![b"a".to_vec(), b"b".to_vec()]);

        let (next2, second_page) = db.scan_live_keys(next, 2);
        assert_eq!(next2, 0);
        assert_eq!(second_page, vec![b"c".to_vec()]);
    }

    #[test]
    fn scan_live_keys_returns_zero_cursor_when_cursor_exceeds_length() {
        let db = engine();
        db.put_entry(b"a", entry("1", None));

        let (next, items) = db.scan_live_keys(10, 2);
        assert_eq!(next, 0);
        assert!(items.is_empty());
    }

    #[test]
    fn scan_live_keys_returns_empty_page_when_count_is_zero() {
        let db = engine();
        db.put_entry(b"a", entry("1", None));
        db.put_entry(b"b", entry("2", None));

        let (next, items) = db.scan_live_keys(0, 0);
        assert_eq!(next, 0);
        assert!(items.is_empty());
    }

    #[test]
    fn flush_all_clears_all_entries() {
        let db = engine();
        db.put_entry(b"a", entry("1", None));
        db.put_entry(b"b", entry("2", None));

        db.flush_all();
        assert!(db.get_entry(b"a").is_none());
        assert!(db.get_entry(b"b").is_none());
        assert!(db.iter_live_keys().is_empty());
    }

    #[test]
    fn new_returns_error_when_worker_count_is_zero() {
        let result = DashMapStorageEngine::new(DbConfig { worker_count: 0 });
        assert!(matches!(result, Err(DbError::InvalidConfig(_))));
    }

    #[test]
    fn contains_live_key_returns_false_when_expire_time_equals_now() {
        let db = engine();
        let now = now_ms_for_test();
        db.put_entry(b"k1", entry("v", Some(now)));

        assert!(!db.contains_live_key(b"k1"));
        assert!(db.get_entry(b"k1").is_none());
    }

    #[test]
    fn concurrent_writes_are_consistent() {
        let db = Arc::new(engine());
        let mut handles = Vec::new();

        for i in 0..8 {
            let db_cloned = Arc::clone(&db);
            handles.push(thread::spawn(move || {
                let value = format!("v{i}");
                db_cloned.put_entry(b"shared", entry(&value, None));
                assert!(db_cloned.contains_live_key(b"shared"));
            }));
        }

        for handle in handles {
            handle.join().expect("writer thread should finish");
        }

        let got = db.get_entry(b"shared");
        assert!(got.is_some());
    }

    #[test]
    fn concurrent_put_if_absent_allows_only_one_success_for_same_key() {
        let db = Arc::new(engine());
        let mut handles = Vec::new();

        for i in 0..16 {
            let db_cloned = Arc::clone(&db);
            handles.push(thread::spawn(move || {
                let value = format!("v{i}");
                db_cloned.put_if_absent(b"once", entry(&value, None))
            }));
        }

        let success_count = handles
            .into_iter()
            .map(|h| h.join().expect("thread should finish"))
            .filter(|ok| *ok)
            .count();

        assert_eq!(success_count, 1);
        assert!(db.get_entry(b"once").is_some());
    }

    #[test]
    fn concurrent_put_if_present_allows_only_one_success_after_expired_cleanup() {
        let db = Arc::new(engine());
        let expired = now_ms_for_test().saturating_sub(1);
        db.put_entry(b"k1", entry("old", Some(expired)));

        let mut handles = Vec::new();
        for i in 0..12 {
            let db_cloned = Arc::clone(&db);
            handles.push(thread::spawn(move || {
                let value = format!("new-{i}");
                db_cloned.put_if_absent(b"k1", entry(&value, None))
            }));
        }

        let success_count = handles
            .into_iter()
            .map(|h| h.join().expect("thread should finish"))
            .filter(|ok| *ok)
            .count();

        assert_eq!(success_count, 1);
        let got = db.get_entry(b"k1").expect("key should exist after replacement");
        assert!(got.value.starts_with(b"new-"));
    }
}
