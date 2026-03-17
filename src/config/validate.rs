use crate::config::{ConfigError, model::AppConfig};

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
    Ok(())
}
