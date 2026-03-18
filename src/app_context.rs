use std::sync::Arc;

use crate::config::AppConfig;
use crate::persistence::aof::AofEngine;
use crate::storage::StorageEngine;

#[derive(Clone)]
pub struct AppContext {
    pub config: AppConfig,
    pub db: Arc<dyn StorageEngine + Send + Sync>,
    pub aof: Option<AofEngine>,
}

impl AppContext {
    pub fn new(
        config: AppConfig,
        db: Arc<dyn StorageEngine + Send + Sync>,
        aof: Option<AofEngine>,
    ) -> Self {
        Self { config, db, aof }
    }
}
