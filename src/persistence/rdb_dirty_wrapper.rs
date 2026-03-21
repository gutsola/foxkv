//! Storage decorator that increments RDB dirty counter on writes.
//! Keeps StorageEngine trait and implementations unchanged.

use std::sync::Arc;

use crate::persistence::rdb::RdbDirtyTracker;
use crate::storage::{StorageEngine, ValueEntry};

/// Wraps a StorageEngine and increments dirty count on each successful write.
#[derive(Clone)]
pub struct StorageWithRdbDirty {
    inner: Arc<dyn StorageEngine + Send + Sync>,
    dirty_tracker: Arc<RdbDirtyTracker>,
}

impl StorageWithRdbDirty {
    pub fn new(
        inner: Arc<dyn StorageEngine + Send + Sync>,
        dirty_tracker: Arc<RdbDirtyTracker>,
    ) -> Self {
        Self { inner, dirty_tracker }
    }
}

impl StorageEngine for StorageWithRdbDirty {
    fn get_entry(&self, key: &[u8]) -> Option<ValueEntry> {
        self.inner.get_entry(key)
    }

    fn put_entry(&self, key: &[u8], entry: ValueEntry) {
        self.inner.put_entry(key, entry);
        self.dirty_tracker.incr_dirty();
    }

    fn put_if_absent(&self, key: &[u8], entry: ValueEntry) -> bool {
        let ok = self.inner.put_if_absent(key, entry);
        if ok {
            self.dirty_tracker.incr_dirty();
        }
        ok
    }

    fn put_if_present(&self, key: &[u8], entry: ValueEntry) -> bool {
        let ok = self.inner.put_if_present(key, entry);
        if ok {
            self.dirty_tracker.incr_dirty();
        }
        ok
    }

    fn remove_entry(&self, key: &[u8]) -> bool {
        let ok = self.inner.remove_entry(key);
        if ok {
            self.dirty_tracker.incr_dirty();
        }
        ok
    }

    fn contains_live_key(&self, key: &[u8]) -> bool {
        self.inner.contains_live_key(key)
    }

    fn iter_live_keys(&self) -> Vec<Vec<u8>> {
        self.inner.iter_live_keys()
    }

    fn scan_live_keys(&self, cursor: usize, count: usize) -> (usize, Vec<Vec<u8>>) {
        self.inner.scan_live_keys(cursor, count)
    }

    fn flush_all(&self) {
        self.inner.flush_all();
        self.dirty_tracker.incr_dirty();
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use bytes::Bytes;

    use super::*;
    use crate::storage::{DashMapStorageEngine, DbConfig, StorageEngine, ValueEntry};

    fn test_db() -> Arc<dyn StorageEngine + Send + Sync> {
        Arc::new(DashMapStorageEngine::new(DbConfig { worker_count: 2 }).expect("db init failed"))
    }

    fn test_tracker() -> Arc<RdbDirtyTracker> {
        Arc::new(RdbDirtyTracker::new())
    }

    fn create_wrapper() -> (StorageWithRdbDirty, Arc<RdbDirtyTracker>) {
        let db = test_db();
        let tracker = test_tracker();
        let wrapper = StorageWithRdbDirty::new(db, tracker.clone());
        (wrapper, tracker)
    }

    #[test]
    fn storage_with_rdb_dirty_get_entry_returns_value_from_inner() {
        let (wrapper, _) = create_wrapper();
        wrapper.put_entry(
            b"key",
            ValueEntry {
                value: Bytes::from("value"),
                expire_at_ms: None,
            },
        );
        let entry = wrapper.get_entry(b"key").expect("key should exist");
        assert_eq!(entry.value.as_ref(), b"value");
    }

    #[test]
    fn storage_with_rdb_dirty_get_entry_returns_none_for_missing_key() {
        let (wrapper, _) = create_wrapper();
        let result = wrapper.get_entry(b"nonexistent");
        assert!(result.is_none());
    }

    #[test]
    fn storage_with_rdb_dirty_put_entry_increments_dirty_counter() {
        let (wrapper, tracker) = create_wrapper();
        assert_eq!(tracker.dirty(), 0);
        wrapper.put_entry(
            b"key",
            ValueEntry {
                value: Bytes::from("value"),
                expire_at_ms: None,
            },
        );
        assert_eq!(tracker.dirty(), 1);
        wrapper.put_entry(
            b"key2",
            ValueEntry {
                value: Bytes::from("value2"),
                expire_at_ms: None,
            },
        );
        assert_eq!(tracker.dirty(), 2);
    }

    #[test]
    fn storage_with_rdb_dirty_put_if_absent_increments_dirty_on_success() {
        let (wrapper, tracker) = create_wrapper();
        assert_eq!(tracker.dirty(), 0);
        let result = wrapper.put_if_absent(
            b"key",
            ValueEntry {
                value: Bytes::from("value"),
                expire_at_ms: None,
            },
        );
        assert!(result);
        assert_eq!(tracker.dirty(), 1);
    }

    #[test]
    fn storage_with_rdb_dirty_put_if_absent_does_not_increment_dirty_on_failure() {
        let (wrapper, tracker) = create_wrapper();
        wrapper.put_entry(
            b"key",
            ValueEntry {
                value: Bytes::from("value"),
                expire_at_ms: None,
            },
        );
        let initial_dirty = tracker.dirty();
        let result = wrapper.put_if_absent(
            b"key",
            ValueEntry {
                value: Bytes::from("newvalue"),
                expire_at_ms: None,
            },
        );
        assert!(!result);
        assert_eq!(tracker.dirty(), initial_dirty);
    }

    #[test]
    fn storage_with_rdb_dirty_put_if_present_increments_dirty_on_success() {
        let (wrapper, tracker) = create_wrapper();
        wrapper.put_entry(
            b"key",
            ValueEntry {
                value: Bytes::from("value"),
                expire_at_ms: None,
            },
        );
        let initial_dirty = tracker.dirty();
        let result = wrapper.put_if_present(
            b"key",
            ValueEntry {
                value: Bytes::from("newvalue"),
                expire_at_ms: None,
            },
        );
        assert!(result);
        assert_eq!(tracker.dirty(), initial_dirty + 1);
    }

    #[test]
    fn storage_with_rdb_dirty_put_if_present_does_not_increment_dirty_on_failure() {
        let (wrapper, tracker) = create_wrapper();
        let initial_dirty = tracker.dirty();
        let result = wrapper.put_if_present(
            b"nonexistent",
            ValueEntry {
                value: Bytes::from("value"),
                expire_at_ms: None,
            },
        );
        assert!(!result);
        assert_eq!(tracker.dirty(), initial_dirty);
    }

    #[test]
    fn storage_with_rdb_dirty_remove_entry_increments_dirty_on_success() {
        let (wrapper, tracker) = create_wrapper();
        wrapper.put_entry(
            b"key",
            ValueEntry {
                value: Bytes::from("value"),
                expire_at_ms: None,
            },
        );
        let initial_dirty = tracker.dirty();
        let result = wrapper.remove_entry(b"key");
        assert!(result);
        assert_eq!(tracker.dirty(), initial_dirty + 1);
    }

    #[test]
    fn storage_with_rdb_dirty_remove_entry_does_not_increment_dirty_on_failure() {
        let (wrapper, tracker) = create_wrapper();
        let initial_dirty = tracker.dirty();
        let result = wrapper.remove_entry(b"nonexistent");
        assert!(!result);
        assert_eq!(tracker.dirty(), initial_dirty);
    }

    #[test]
    fn storage_with_rdb_dirty_contains_live_key_returns_correct_result() {
        let (wrapper, _) = create_wrapper();
        assert!(!wrapper.contains_live_key(b"key"));
        wrapper.put_entry(
            b"key",
            ValueEntry {
                value: Bytes::from("value"),
                expire_at_ms: None,
            },
        );
        assert!(wrapper.contains_live_key(b"key"));
    }

    #[test]
    fn storage_with_rdb_dirty_iter_live_keys_returns_all_keys() {
        let (wrapper, _) = create_wrapper();
        wrapper.put_entry(
            b"key1",
            ValueEntry {
                value: Bytes::from("value1"),
                expire_at_ms: None,
            },
        );
        wrapper.put_entry(
            b"key2",
            ValueEntry {
                value: Bytes::from("value2"),
                expire_at_ms: None,
            },
        );
        let keys = wrapper.iter_live_keys();
        assert_eq!(keys.len(), 2);
        assert!(keys.iter().any(|k| k == b"key1"));
        assert!(keys.iter().any(|k| k == b"key2"));
    }

    #[test]
    fn storage_with_rdb_dirty_scan_live_keys_returns_paginated_keys() {
        let (wrapper, _) = create_wrapper();
        for i in 0..10 {
            let key = format!("key{}", i);
            wrapper.put_entry(
                key.as_bytes(),
                ValueEntry {
                    value: Bytes::from("value"),
                    expire_at_ms: None,
                },
            );
        }
        let (cursor, keys) = wrapper.scan_live_keys(0, 5);
        assert!(cursor > 0);
        assert!(keys.len() <= 5);
    }

    #[test]
    fn storage_with_rdb_dirty_flush_all_increments_dirty_counter() {
        let (wrapper, tracker) = create_wrapper();
        let initial_dirty = tracker.dirty();
        wrapper.flush_all();
        assert_eq!(tracker.dirty(), initial_dirty + 1);
    }

    #[test]
    fn storage_with_rdb_dirty_flush_all_clears_all_keys() {
        let (wrapper, _) = create_wrapper();
        wrapper.put_entry(
            b"key1",
            ValueEntry {
                value: Bytes::from("value1"),
                expire_at_ms: None,
            },
        );
        wrapper.put_entry(
            b"key2",
            ValueEntry {
                value: Bytes::from("value2"),
                expire_at_ms: None,
            },
        );
        wrapper.flush_all();
        assert!(wrapper.get_entry(b"key1").is_none());
        assert!(wrapper.get_entry(b"key2").is_none());
    }

    #[test]
    fn storage_with_rdb_dirty_put_entry_overwrites_existing_key() {
        let (wrapper, tracker) = create_wrapper();
        wrapper.put_entry(
            b"key",
            ValueEntry {
                value: Bytes::from("value1"),
                expire_at_ms: None,
            },
        );
        wrapper.put_entry(
            b"key",
            ValueEntry {
                value: Bytes::from("value2"),
                expire_at_ms: None,
            },
        );
        let entry = wrapper.get_entry(b"key").expect("key should exist");
        assert_eq!(entry.value.as_ref(), b"value2");
        assert_eq!(tracker.dirty(), 2);
    }

    #[test]
    fn storage_with_rdb_dirty_preserves_expire_at_ms() {
        let (wrapper, _) = create_wrapper();
        wrapper.put_entry(
            b"key",
            ValueEntry {
                value: Bytes::from("value"),
                expire_at_ms: Some(9999999999999),
            },
        );
        let entry = wrapper.get_entry(b"key").expect("key should exist");
        assert_eq!(entry.expire_at_ms, Some(9999999999999));
    }

    #[test]
    fn storage_with_rdb_dirty_new_creates_wrapper_with_correct_inner() {
        let db = test_db();
        let tracker = test_tracker();
        db.put_entry(
            b"existing",
            ValueEntry {
                value: Bytes::from("data"),
                expire_at_ms: None,
            },
        );
        let wrapper = StorageWithRdbDirty::new(db, tracker.clone());
        let entry = wrapper.get_entry(b"existing").expect("key should exist");
        assert_eq!(entry.value.as_ref(), b"data");
        assert_eq!(tracker.dirty(), 0);
    }
}
