use crate::config::{
    ConfigError,
    model::{AppConfig, ReplicationConfig},
};

pub fn validate_config(config: &AppConfig) -> Result<(), ConfigError> {
    if config.bind.is_empty() {
        return Err(ConfigError::Validate(
            "bind must contain at least one address".to_string(),
        ));
    }
    if config.bind.iter().any(|b| b.trim().is_empty()) {
        return Err(ConfigError::Validate(
            "bind contains empty address".to_string(),
        ));
    }
    if config.port == 0 {
        return Err(ConfigError::Validate(
            "port must be in range 1..=65535".to_string(),
        ));
    }
    for rule in &config.rdb.save_rules {
        if rule.seconds == 0 || rule.changes == 0 {
            return Err(ConfigError::Validate(
                "save rule requires positive seconds and changes".to_string(),
            ));
        }
    }
    if config.rdb.dbfilename.trim().is_empty() {
        return Err(ConfigError::Validate(
            "dbfilename must not be empty".to_string(),
        ));
    }
    if config.aof.appendfilename.trim().is_empty() {
        return Err(ConfigError::Validate(
            "appendfilename must not be empty".to_string(),
        ));
    }
    if matches!(config.worker_threads, Some(0)) {
        return Err(ConfigError::Validate(
            "worker-threads must be greater than 0".to_string(),
        ));
    }
    if let ReplicationConfig::Replica { host, port } = &config.replication {
        if host.trim().is_empty() {
            return Err(ConfigError::Validate(
                "replicaof host must not be empty".to_string(),
            ));
        }
        if *port == 0 {
            return Err(ConfigError::Validate(
                "replicaof port must be in range 1..=65535".to_string(),
            ));
        }
    }
    Ok(())
}
