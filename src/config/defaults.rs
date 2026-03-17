use std::path::PathBuf;

use crate::config::model::{AofConfig, AppConfig, AppendFsyncPolicy, RdbConfig, SaveRule};

pub fn default_config() -> AppConfig {
    AppConfig {
        bind: vec!["127.0.0.1".to_string()],
        port: 6380,
        rdb: RdbConfig {
            save_rules: vec![
                SaveRule {
                    seconds: 900,
                    changes: 1,
                },
                SaveRule {
                    seconds: 300,
                    changes: 10,
                },
                SaveRule {
                    seconds: 60,
                    changes: 10_000,
                },
            ],
            dbfilename: "dump.rdb".to_string(),
            dir: PathBuf::from("./"),
        },
        aof: AofConfig {
            enabled: false,
            appendfilename: "appendonly.aof".to_string(),
            appendfsync: AppendFsyncPolicy::EverySec,
            auto_rewrite_percentage: 100,
            auto_rewrite_min_size_bytes: 64 * 1024 * 1024,
            use_rdb_preamble: true,
        },
    }
}
