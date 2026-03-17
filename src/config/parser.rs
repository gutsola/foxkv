use std::path::PathBuf;

use crate::config::ConfigError;
use crate::config::model::{AppConfig, AppendFsyncPolicy, SaveRule};

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
            "save" => {
                if !seen_save {
                    config.rdb.save_rules.clear();
                    seen_save = true;
                }
                if tokens.len() == 2 && tokens[1].is_empty() {
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
