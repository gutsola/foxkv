use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use ahash::RandomState;
use dashmap::DashMap;

#[derive(Debug, Clone, Copy)]
pub struct ConcurrentMapConfig {
    pub worker_count: usize,
}

#[derive(Debug)]
pub enum ConcurrentMapError {
    InvalidConfig(&'static str),
}

impl std::fmt::Display for ConcurrentMapError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ConcurrentMapError::InvalidConfig(msg) => write!(f, "invalid config: {msg}"),
        }
    }
}

impl std::error::Error for ConcurrentMapError {}

#[derive(Clone)]
pub struct ConcurrentMapDb {
    inner: Arc<DashMap<Vec<u8>, Entry, RandomState>>,
}

#[derive(Clone)]
struct Entry {
    value: Vec<u8>,
    expire_at_ms: Option<u64>,
}

impl ConcurrentMapDb {
    pub fn new(config: ConcurrentMapConfig) -> Result<Self, ConcurrentMapError> {
        validate_config(config)?;
        let shard_amount = config.worker_count.max(2).next_power_of_two();
        let map = DashMap::with_hasher_and_shard_amount(RandomState::new(), shard_amount);
        Ok(Self {
            inner: Arc::new(map),
        })
    }

    pub fn set(&self, key: &[u8], value: &[u8]) {
        self.set_with_optional_ttl_ms(key, value, None);
    }

    pub fn set_with_optional_ttl_ms(&self, key: &[u8], value: &[u8], ttl_ms: Option<u64>) {
        let now = now_ms();
        self.insert_entry(key, value, ttl_ms, now);
    }

    pub fn set_with_ttl_ms(&self, key: &[u8], value: &[u8], ttl_ms: u64) {
        self.set_with_optional_ttl_ms(key, value, Some(ttl_ms));
    }

    pub fn set_nx_with_optional_ttl_ms(
        &self,
        key: &[u8],
        value: &[u8],
        ttl_ms: Option<u64>,
    ) -> bool {
        let now = now_ms();
        if self.exists_at(key, now) {
            return false;
        }
        self.insert_entry(key, value, ttl_ms, now);
        true
    }

    pub fn set_xx_with_optional_ttl_ms(
        &self,
        key: &[u8],
        value: &[u8],
        ttl_ms: Option<u64>,
    ) -> bool {
        let now = now_ms();
        if let Some(mut entry) = self.inner.get_mut(key) {
            if entry
                .expire_at_ms
                .is_some_and(|expire_at_ms| expire_at_ms <= now)
            {
                drop(entry);
                self.inner.remove(key);
                return false;
            }
            entry.value = value.to_vec();
            entry.expire_at_ms = ttl_ms.map(|ttl| now.saturating_add(ttl));
            return true;
        }
        false
    }

    pub fn get_set(&self, key: &[u8], value: &[u8]) -> Option<Vec<u8>> {
        let old = self.get(key);
        self.set(key, value);
        old
    }

    pub fn mset_nx(&self, pairs: &[(&[u8], &[u8])]) -> bool {
        if pairs.iter().any(|(key, _)| self.exists(key)) {
            return false;
        }
        for (key, value) in pairs {
            self.set(key, value);
        }
        true
    }

    pub fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        let now = now_ms();
        if let Some(entry) = self.inner.get(key) {
            if is_expired(&entry, now) {
                drop(entry);
                self.inner.remove(key);
                return None;
            }
            return Some(entry.value.clone());
        }
        None
    }

    pub fn ttl_seconds(&self, key: &[u8]) -> i64 {
        let now = now_ms();
        if let Some(entry) = self.inner.get(key) {
            if let Some(expire_at_ms) = entry.expire_at_ms {
                if expire_at_ms <= now {
                    drop(entry);
                    self.inner.remove(key);
                    return -2;
                }
                return ((expire_at_ms - now) / 1000) as i64;
            }
            return -1;
        }
        -2
    }

    pub fn exists(&self, key: &[u8]) -> bool {
        self.exists_at(key, now_ms())
    }

    pub fn delete(&self, key: &[u8]) -> bool {
        self.inner.remove(key).is_some()
    }

    pub fn len(&self) -> usize {
        self.inner.len()
    }

    pub fn dbsize(&self) -> usize {
        self.collect_live_keys().len()
    }

    pub fn scan(&self, cursor: usize, count: usize) -> (usize, Vec<Vec<u8>>) {
        let mut keys = self.collect_live_keys();
        keys.sort();

        let start = cursor.min(keys.len());
        let end = start.saturating_add(count).min(keys.len());
        let next_cursor = if end >= keys.len() { 0 } else { end };
        let items = keys[start..end].to_vec();
        (next_cursor, items)
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

    fn insert_entry(&self, key: &[u8], value: &[u8], ttl_ms: Option<u64>, now: u64) {
        self.inner.insert(
            key.to_vec(),
            Entry {
                value: value.to_vec(),
                expire_at_ms: ttl_ms.map(|ttl| now.saturating_add(ttl)),
            },
        );
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

fn validate_config(config: ConcurrentMapConfig) -> Result<(), ConcurrentMapError> {
    if config.worker_count == 0 {
        return Err(ConcurrentMapError::InvalidConfig(
            "worker_count must be > 0",
        ));
    }
    Ok(())
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

fn is_expired(entry: &Entry, now: u64) -> bool {
    entry
        .expire_at_ms
        .is_some_and(|expire_at_ms| expire_at_ms <= now)
}
