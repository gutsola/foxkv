use bytes::Bytes;

#[derive(Debug, Clone, Copy)]
pub struct DbConfig {
    pub worker_count: usize,
}

#[derive(Debug)]
pub enum DbError {
    InvalidConfig(&'static str),
}

impl std::fmt::Display for DbError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DbError::InvalidConfig(msg) => write!(f, "invalid config: {msg}"),
        }
    }
}

impl std::error::Error for DbError {}

#[derive(Clone)]
pub struct ValueEntry {
    pub value: Bytes,
    pub expire_at_ms: Option<u64>,
}
