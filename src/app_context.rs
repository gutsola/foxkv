use crate::config::AppConfig;
use crate::persistence::aof::AofEngine;
use crate::storage::ConcurrentMapDb;

#[derive(Clone)]
pub struct AppContext {
    pub config: AppConfig,
    pub db: ConcurrentMapDb,
    pub aof: Option<AofEngine>,
}

impl AppContext {
    pub fn new(config: AppConfig, db: ConcurrentMapDb, aof: Option<AofEngine>) -> Self {
        Self { config, db, aof }
    }
}
