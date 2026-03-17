use std::fmt;
use std::path::Path;

mod defaults;
pub mod model;
mod parser;
mod validate;

pub use defaults::default_config;
pub use model::AppConfig;

#[derive(Debug)]
pub enum ConfigError {
    Io(std::io::Error),
    Parse(String),
    Validate(String),
}

impl fmt::Display for ConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ConfigError::Io(err) => write!(f, "io error: {err}"),
            ConfigError::Parse(msg) => write!(f, "parse error: {msg}"),
            ConfigError::Validate(msg) => write!(f, "validate error: {msg}"),
        }
    }
}

impl std::error::Error for ConfigError {}

impl From<std::io::Error> for ConfigError {
    fn from(value: std::io::Error) -> Self {
        Self::Io(value)
    }
}

pub fn load_from_path(path: &Path) -> Result<AppConfig, ConfigError> {
    let content = std::fs::read_to_string(path)?;
    let mut config = default_config();
    parser::apply_redis_conf(&content, &mut config)?;
    validate::validate_config(&config)?;
    Ok(config)
}
