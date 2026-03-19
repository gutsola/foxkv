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
