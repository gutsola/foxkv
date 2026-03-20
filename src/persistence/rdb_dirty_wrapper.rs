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
