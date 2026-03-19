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

/// 单类客户端的输出缓冲区限制
/// client-output-buffer-limit <class> <hard limit> <soft limit> <soft seconds>
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClientOutputBufferLimit {
    pub hard_limit_bytes: u64,
    pub soft_limit_bytes: u64,
    pub soft_seconds: u32,
}

/// 三类客户端的输出缓冲区限制：normal、replica、pubsub
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClientOutputBufferLimits {
    pub normal: ClientOutputBufferLimit,
    pub replica: ClientOutputBufferLimit,
    pub pubsub: ClientOutputBufferLimit,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RdbConfig {
    pub save_rules: Vec<SaveRule>,
    pub dbfilename: String,
    pub dir: PathBuf,
    /// stop-writes-on-bgsave-error
    pub stop_writes_on_bgsave_error: bool,
    /// rdbcompression
    pub rdbcompression: bool,
    /// rdbchecksum
    pub rdbchecksum: bool,
    /// rdb-save-incremental-fsync
    pub rdb_save_incremental_fsync: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AofConfig {
    pub enabled: bool,
    pub appendfilename: String,
    pub appendfsync: AppendFsyncPolicy,
    pub auto_rewrite_percentage: u32,
    pub auto_rewrite_min_size_bytes: u64,
    pub use_rdb_preamble: bool,
    /// aof-rewrite-incremental-fsync
    pub aof_rewrite_incremental_fsync: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppConfig {
    pub bind: Vec<String>,
    pub port: u16,
    pub rdb: RdbConfig,
    pub aof: AofConfig,
    /// requirepass，None 表示未设置密码
    pub requirepass: Option<String>,
    /// maxclients，None 表示使用默认（如 10000）
    pub maxclients: Option<u32>,
    /// client-output-buffer-limit
    pub client_output_buffer_limits: ClientOutputBufferLimits,
    /// lua-time-limit，单位毫秒
    pub lua_time_limit: u32,
    /// hz，后台任务执行频率
    pub hz: u32,
}

impl AppConfig {
    pub fn listen_addr(&self) -> String {
        let bind = self.bind.first().map(String::as_str).unwrap_or("127.0.0.1");
        format!("{bind}:{}", self.port)
    }
}
