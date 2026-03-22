use std::path::PathBuf;

use crate::config::model::{
    AofConfig, AppConfig, AppendFsyncPolicy, ClientOutputBufferLimit, ClientOutputBufferLimits,
    RdbConfig, ReplicationConfig,
};

pub fn default_config() -> AppConfig {
    AppConfig {
        bind: vec!["127.0.0.1".to_string()],
        port: 6379,
        rdb: RdbConfig {
            save_rules: vec![],
            dbfilename: "dump.rdb".to_string(),
            dir: PathBuf::from("./"),
            stop_writes_on_bgsave_error: true,
            rdbcompression: true,
            rdbchecksum: true,
            rdb_save_incremental_fsync: true,
        },
        aof: AofConfig {
            enabled: false,
            appendfilename: "appendonly.aof".to_string(),
            appendfsync: AppendFsyncPolicy::EverySec,
            auto_rewrite_percentage: 100,
            auto_rewrite_min_size_bytes: 64 * 1024 * 1024,
            use_rdb_preamble: true,
            aof_rewrite_incremental_fsync: true,
        },
        requirepass: None,
        maxclients: None,
        client_output_buffer_limits: ClientOutputBufferLimits {
            normal: ClientOutputBufferLimit {
                hard_limit_bytes: 0,
                soft_limit_bytes: 0,
                soft_seconds: 0,
            },
            replica: ClientOutputBufferLimit {
                hard_limit_bytes: 256 * 1024 * 1024,
                soft_limit_bytes: 64 * 1024 * 1024,
                soft_seconds: 60,
            },
            pubsub: ClientOutputBufferLimit {
                hard_limit_bytes: 32 * 1024 * 1024,
                soft_limit_bytes: 8 * 1024 * 1024,
                soft_seconds: 60,
            },
        },
        lua_time_limit: 5000,
        hz: 1,
        worker_threads: None,
        replication: ReplicationConfig::Master,
    }
}
