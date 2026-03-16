use std::sync::Arc;

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
    inner: Arc<DashMap<Vec<u8>, Vec<u8>, RandomState>>,
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
        self.inner.insert(key.to_vec(), value.to_vec());
    }

    pub fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        self.inner.get(key).map(|v| v.clone())
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

