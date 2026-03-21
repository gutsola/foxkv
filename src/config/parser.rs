use std::path::PathBuf;

use crate::config::ConfigError;
use crate::config::model::{
    AppConfig, AppendFsyncPolicy, ClientOutputBufferLimit, ReplicationConfig, SaveRule,
};

pub fn apply_redis_conf(content: &str, config: &mut AppConfig) -> Result<(), ConfigError> {
    let mut seen_save = false;
    for (line_no, raw_line) in content.lines().enumerate() {
        let tokens = split_tokens(raw_line)
            .map_err(|e| ConfigError::Parse(format!("line {}: {e}", line_no + 1)))?;
        if tokens.is_empty() {
            continue;
        }
        let key = tokens[0].to_ascii_lowercase();
        match key.as_str() {
            "bind" => {
                if tokens.len() < 2 {
                    return Err(ConfigError::Parse(format!(
                        "line {}: bind requires at least one address",
                        line_no + 1
                    )));
                }
                config.bind = tokens[1..].to_vec();
            }
            "port" => {
                if tokens.len() != 2 {
                    return Err(ConfigError::Parse(format!(
                        "line {}: port requires exactly one value",
                        line_no + 1
                    )));
                }
                config.port = parse_u16(&tokens[1], "port", line_no + 1)?;
            }
            "replicaof" | "slaveof" => {
                if tokens.len() != 3 {
                    return Err(ConfigError::Parse(format!(
                        "line {}: {} expects <host> <port> or 'no one'",
                        line_no + 1,
                        key
                    )));
                }
                if tokens[1].eq_ignore_ascii_case("no") && tokens[2].eq_ignore_ascii_case("one") {
                    config.replication = ReplicationConfig::Master;
                    continue;
                }
                let host = tokens[1].trim();
                if host.is_empty() {
                    return Err(ConfigError::Parse(format!(
                        "line {}: {} host must not be empty",
                        line_no + 1,
                        key
                    )));
                }
                let port = parse_u16(&tokens[2], "replicaof port", line_no + 1)?;
                config.replication = ReplicationConfig::Replica {
                    host: host.to_string(),
                    port,
                };
            }
            "save" => {
                if !seen_save {
                    config.rdb.save_rules.clear();
                    seen_save = true;
                }
                if tokens.len() == 2 && tokens[1].is_empty() {
                    // save "" removes all previously configured save points (Redis semantics)
                    config.rdb.save_rules.clear();
                    continue;
                }
                if tokens.len() != 3 {
                    return Err(ConfigError::Parse(format!(
                        "line {}: save expects two numbers (seconds changes)",
                        line_no + 1
                    )));
                }
                let seconds = parse_u64(&tokens[1], "save seconds", line_no + 1)?;
                let changes = parse_u64(&tokens[2], "save changes", line_no + 1)?;
                config.rdb.save_rules.push(SaveRule { seconds, changes });
            }
            "dbfilename" => {
                if tokens.len() != 2 {
                    return Err(ConfigError::Parse(format!(
                        "line {}: dbfilename requires one value",
                        line_no + 1
                    )));
                }
                config.rdb.dbfilename = tokens[1].clone();
            }
            "dir" => {
                if tokens.len() != 2 {
                    return Err(ConfigError::Parse(format!(
                        "line {}: dir requires one value",
                        line_no + 1
                    )));
                }
                config.rdb.dir = PathBuf::from(tokens[1].clone());
            }
            "stop-writes-on-bgsave-error" => {
                if tokens.len() != 2 {
                    return Err(ConfigError::Parse(format!(
                        "line {}: stop-writes-on-bgsave-error requires one value",
                        line_no + 1
                    )));
                }
                config.rdb.stop_writes_on_bgsave_error =
                    parse_yes_no(&tokens[1], "stop-writes-on-bgsave-error", line_no + 1)?;
            }
            "rdbcompression" => {
                if tokens.len() != 2 {
                    return Err(ConfigError::Parse(format!(
                        "line {}: rdbcompression requires one value",
                        line_no + 1
                    )));
                }
                config.rdb.rdbcompression =
                    parse_yes_no(&tokens[1], "rdbcompression", line_no + 1)?;
            }
            "rdbchecksum" => {
                if tokens.len() != 2 {
                    return Err(ConfigError::Parse(format!(
                        "line {}: rdbchecksum requires one value",
                        line_no + 1
                    )));
                }
                config.rdb.rdbchecksum =
                    parse_yes_no(&tokens[1], "rdbchecksum", line_no + 1)?;
            }
            "rdb-save-incremental-fsync" => {
                if tokens.len() != 2 {
                    return Err(ConfigError::Parse(format!(
                        "line {}: rdb-save-incremental-fsync requires one value",
                        line_no + 1
                    )));
                }
                config.rdb.rdb_save_incremental_fsync =
                    parse_yes_no(&tokens[1], "rdb-save-incremental-fsync", line_no + 1)?;
            }
            "appendonly" => {
                if tokens.len() != 2 {
                    return Err(ConfigError::Parse(format!(
                        "line {}: appendonly requires one value",
                        line_no + 1
                    )));
                }
                config.aof.enabled = parse_yes_no(&tokens[1], "appendonly", line_no + 1)?;
            }
            "appendfilename" => {
                if tokens.len() != 2 {
                    return Err(ConfigError::Parse(format!(
                        "line {}: appendfilename requires one value",
                        line_no + 1
                    )));
                }
                config.aof.appendfilename = tokens[1].clone();
            }
            "appendfsync" => {
                if tokens.len() != 2 {
                    return Err(ConfigError::Parse(format!(
                        "line {}: appendfsync requires one value",
                        line_no + 1
                    )));
                }
                config.aof.appendfsync = parse_appendfsync(&tokens[1], line_no + 1)?;
            }
            "auto-aof-rewrite-percentage" => {
                if tokens.len() != 2 {
                    return Err(ConfigError::Parse(format!(
                        "line {}: auto-aof-rewrite-percentage requires one value",
                        line_no + 1
                    )));
                }
                config.aof.auto_rewrite_percentage =
                    parse_u32(&tokens[1], "auto-aof-rewrite-percentage", line_no + 1)?;
            }
            "auto-aof-rewrite-min-size" => {
                if tokens.len() != 2 {
                    return Err(ConfigError::Parse(format!(
                        "line {}: auto-aof-rewrite-min-size requires one value",
                        line_no + 1
                    )));
                }
                config.aof.auto_rewrite_min_size_bytes = parse_size_bytes(&tokens[1], line_no + 1)?;
            }
            "aof-use-rdb-preamble" => {
                if tokens.len() != 2 {
                    return Err(ConfigError::Parse(format!(
                        "line {}: aof-use-rdb-preamble requires one value",
                        line_no + 1
                    )));
                }
                config.aof.use_rdb_preamble =
                    parse_yes_no(&tokens[1], "aof-use-rdb-preamble", line_no + 1)?;
            }
            "aof-rewrite-incremental-fsync" => {
                if tokens.len() != 2 {
                    return Err(ConfigError::Parse(format!(
                        "line {}: aof-rewrite-incremental-fsync requires one value",
                        line_no + 1
                    )));
                }
                config.aof.aof_rewrite_incremental_fsync =
                    parse_yes_no(&tokens[1], "aof-rewrite-incremental-fsync", line_no + 1)?;
            }
            "requirepass" => {
                if tokens.len() != 2 {
                    return Err(ConfigError::Parse(format!(
                        "line {}: requirepass requires one value",
                        line_no + 1
                    )));
                }
                config.requirepass = Some(tokens[1].clone());
            }
            "maxclients" => {
                if tokens.len() != 2 {
                    return Err(ConfigError::Parse(format!(
                        "line {}: maxclients requires one value",
                        line_no + 1
                    )));
                }
                config.maxclients = Some(parse_u32(&tokens[1], "maxclients", line_no + 1)?);
            }
            "client-output-buffer-limit" => {
                if tokens.len() != 5 {
                    return Err(ConfigError::Parse(format!(
                        "line {}: client-output-buffer-limit expects <class> <hard> <soft> <soft_seconds>",
                        line_no + 1
                    )));
                }
                let class = tokens[1].to_ascii_lowercase();
                let limit = ClientOutputBufferLimit {
                    hard_limit_bytes: parse_size_bytes(&tokens[2], line_no + 1)?,
                    soft_limit_bytes: parse_size_bytes(&tokens[3], line_no + 1)?,
                    soft_seconds: parse_u32(&tokens[4], "client-output-buffer-limit soft_seconds", line_no + 1)?,
                };
                match class.as_str() {
                    "normal" => config.client_output_buffer_limits.normal = limit,
                    "replica" => config.client_output_buffer_limits.replica = limit,
                    "pubsub" => config.client_output_buffer_limits.pubsub = limit,
                    _ => {
                        return Err(ConfigError::Parse(format!(
                            "line {}: client-output-buffer-limit class must be normal/replica/pubsub, got '{}'",
                            line_no + 1, tokens[1]
                        )));
                    }
                }
            }
            "lua-time-limit" => {
                if tokens.len() != 2 {
                    return Err(ConfigError::Parse(format!(
                        "line {}: lua-time-limit requires one value",
                        line_no + 1
                    )));
                }
                config.lua_time_limit = parse_u32(&tokens[1], "lua-time-limit", line_no + 1)?;
            }
            "hz" => {
                if tokens.len() != 2 {
                    return Err(ConfigError::Parse(format!(
                        "line {}: hz requires one value",
                        line_no + 1
                    )));
                }
                config.hz = parse_u32(&tokens[1], "hz", line_no + 1)?;
            }
            "worker-threads" => {
                if tokens.len() != 2 {
                    return Err(ConfigError::Parse(format!(
                        "line {}: worker-threads requires one value",
                        line_no + 1
                    )));
                }
                config.worker_threads =
                    Some(parse_usize(&tokens[1], "worker-threads", line_no + 1)?);
            }
            _ => {}
        }
    }
    Ok(())
}

fn split_tokens(line: &str) -> Result<Vec<String>, String> {
    let mut out = Vec::new();
    let mut current = String::new();
    let mut token_active = false;
    let mut in_quote = false;
    let mut quote_char = '\0';
    let mut escaped = false;

    for ch in line.chars() {
        if escaped {
            current.push(ch);
            token_active = true;
            escaped = false;
            continue;
        }
        if ch == '\\' && in_quote {
            escaped = true;
            continue;
        }
        if in_quote {
            if ch == quote_char {
                in_quote = false;
            } else {
                current.push(ch);
                token_active = true;
            }
            continue;
        }
        if ch == '"' || ch == '\'' {
            in_quote = true;
            quote_char = ch;
            token_active = true;
            continue;
        }
        if ch == '#' {
            break;
        }
        if ch.is_whitespace() {
            if token_active {
                out.push(std::mem::take(&mut current));
                token_active = false;
            }
            continue;
        }
        current.push(ch);
        token_active = true;
    }

    if in_quote {
        return Err("unterminated quote".to_string());
    }
    if escaped {
        return Err("dangling escape".to_string());
    }
    if token_active {
        out.push(current);
    }
    Ok(out)
}

fn parse_yes_no(raw: &str, key: &str, line_no: usize) -> Result<bool, ConfigError> {
    match raw.to_ascii_lowercase().as_str() {
        "yes" => Ok(true),
        "no" => Ok(false),
        _ => Err(ConfigError::Parse(format!(
            "line {}: {} expects yes/no, got '{}'",
            line_no, key, raw
        ))),
    }
}

fn parse_appendfsync(raw: &str, line_no: usize) -> Result<AppendFsyncPolicy, ConfigError> {
    match raw.to_ascii_lowercase().as_str() {
        "always" => Ok(AppendFsyncPolicy::Always),
        "everysec" => Ok(AppendFsyncPolicy::EverySec),
        "no" => Ok(AppendFsyncPolicy::No),
        _ => Err(ConfigError::Parse(format!(
            "line {}: appendfsync expects always/everysec/no, got '{}'",
            line_no, raw
        ))),
    }
}

fn parse_u16(raw: &str, key: &str, line_no: usize) -> Result<u16, ConfigError> {
    raw.parse::<u16>().map_err(|_| {
        ConfigError::Parse(format!(
            "line {}: {} expects u16 integer, got '{}'",
            line_no, key, raw
        ))
    })
}

fn parse_u32(raw: &str, key: &str, line_no: usize) -> Result<u32, ConfigError> {
    raw.parse::<u32>().map_err(|_| {
        ConfigError::Parse(format!(
            "line {}: {} expects u32 integer, got '{}'",
            line_no, key, raw
        ))
    })
}

fn parse_u64(raw: &str, key: &str, line_no: usize) -> Result<u64, ConfigError> {
    raw.parse::<u64>().map_err(|_| {
        ConfigError::Parse(format!(
            "line {}: {} expects u64 integer, got '{}'",
            line_no, key, raw
        ))
    })
}

fn parse_usize(raw: &str, key: &str, line_no: usize) -> Result<usize, ConfigError> {
    raw.parse::<usize>().map_err(|_| {
        ConfigError::Parse(format!(
            "line {}: {} expects usize integer, got '{}'",
            line_no, key, raw
        ))
    })
}

fn parse_size_bytes(raw: &str, line_no: usize) -> Result<u64, ConfigError> {
    let lower = raw.to_ascii_lowercase();
    if lower.is_empty() {
        return Err(ConfigError::Parse(format!(
            "line {}: size value is empty",
            line_no
        )));
    }

    let (num_part, factor) = if let Some(num) = lower.strip_suffix("kb") {
        (num, 1024_u64)
    } else if let Some(num) = lower.strip_suffix("mb") {
        (num, 1024_u64 * 1024)
    } else if let Some(num) = lower.strip_suffix("gb") {
        (num, 1024_u64 * 1024 * 1024)
    } else {
        (lower.as_str(), 1_u64)
    };

    let num = num_part
        .trim()
        .parse::<u64>()
        .map_err(|_| ConfigError::Parse(format!("line {}: invalid size '{}'", line_no, raw)))?;
    num.checked_mul(factor)
        .ok_or_else(|| ConfigError::Parse(format!("line {}: size overflow '{}'", line_no, raw)))
}

#[cfg(test)]
mod tests {
    use super::apply_redis_conf;
    use crate::config::{default_config, model::ReplicationConfig};

    #[test]
    fn parse_replicaof_sets_replica_role() {
        let mut cfg = default_config();
        apply_redis_conf("replicaof 10.0.0.8 6380", &mut cfg).expect("parse replicaof");
        assert_eq!(
            cfg.replication,
            ReplicationConfig::Replica {
                host: "10.0.0.8".to_string(),
                port: 6380,
            }
        );
    }

    #[test]
    fn parse_slaveof_no_one_resets_master_role() {
        let mut cfg = default_config();
        apply_redis_conf("replicaof 10.0.0.8 6380", &mut cfg).expect("parse replicaof");
        apply_redis_conf("slaveof no one", &mut cfg).expect("parse slaveof no one");
        assert_eq!(cfg.replication, ReplicationConfig::Master);
    }

    #[test]
    fn parse_worker_threads_sets_runtime_threads() {
        let mut cfg = default_config();
        apply_redis_conf("worker-threads 12", &mut cfg).expect("parse worker-threads");
        assert_eq!(cfg.worker_threads, Some(12));
    }
}
