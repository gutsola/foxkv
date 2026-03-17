use std::path::PathBuf;

use crate::config::model::RdbConfig;

#[derive(Debug, Clone)]
pub struct RdbRuntimeConfig {
    pub file_path: PathBuf,
}

impl RdbRuntimeConfig {
    pub fn from_config(cfg: &RdbConfig) -> Self {
        Self {
            file_path: cfg.dir.join(&cfg.dbfilename),
        }
    }
}
