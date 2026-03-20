use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use crate::config::AppConfig;
use crate::persistence::aof::AofEngine;
use crate::persistence::rdb::RdbDirtyTracker;
use crate::replication::ReplicationManager;
use crate::storage::StorageEngine;

#[derive(Clone)]
pub struct AppContext {
    pub config: AppConfig,
    pub db: Arc<dyn StorageEngine + Send + Sync>,
    pub aof: Option<AofEngine>,
    pub rdb_dirty_tracker: Option<Arc<RdbDirtyTracker>>,
    pub rdb_bgsave_in_progress: Option<Arc<AtomicBool>>,
    pub replication: Arc<ReplicationManager>,
}

impl AppContext {
    pub fn new(
        config: AppConfig,
        db: Arc<dyn StorageEngine + Send + Sync>,
        aof: Option<AofEngine>,
        rdb_dirty_tracker: Option<Arc<RdbDirtyTracker>>,
        rdb_bgsave_in_progress: Option<Arc<AtomicBool>>,
        replication: Arc<ReplicationManager>,
    ) -> Self {
        Self {
            config,
            db,
            aof,
            rdb_dirty_tracker,
            rdb_bgsave_in_progress,
            replication,
        }
    }
}
