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
        self.inner.insert(
            key.to_vec(),
            Entry {
                value: value.to_vec(),
                expire_at_ms: None,
            },
        );
    }

    pub fn set_with_ttl_ms(&self, key: &[u8], value: &[u8], ttl_ms: u64) {
        let expire_at_ms = now_ms().saturating_add(ttl_ms);
        self.inner.insert(
            key.to_vec(),
            Entry {
                value: value.to_vec(),
                expire_at_ms: Some(expire_at_ms),
            },
        );
    }

    pub fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        let now = now_ms();
        if let Some(entry) = self.inner.get(key) {
            if entry
                .expire_at_ms
                .is_some_and(|expire_at_ms| expire_at_ms <= now)
            {
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

    pub fn delete(&self, key: &[u8]) -> bool {
        self.inner.remove(key).is_some()
    }

    pub fn len(&self) -> usize {
        self.inner.len()
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

