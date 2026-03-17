use std::path::PathBuf;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SaveRule {
    pub seconds: u64,
    pub changes: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AppendFsyncPolicy {
    Always,
    EverySec,
    No,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RdbConfig {
    pub save_rules: Vec<SaveRule>,
    pub dbfilename: String,
    pub dir: PathBuf,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AofConfig {
    pub enabled: bool,
    pub appendfilename: String,
    pub appendfsync: AppendFsyncPolicy,
    pub auto_rewrite_percentage: u32,
    pub auto_rewrite_min_size_bytes: u64,
    pub use_rdb_preamble: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppConfig {
    pub bind: Vec<String>,
    pub port: u16,
    pub rdb: RdbConfig,
    pub aof: AofConfig,
}

impl AppConfig {
    pub fn listen_addr(&self) -> String {
        let bind = self.bind.first().map(String::as_str).unwrap_or("127.0.0.1");
        format!("{bind}:{}", self.port)
    }
}
