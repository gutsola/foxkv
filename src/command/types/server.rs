use std::time::{SystemTime, UNIX_EPOCH};

use crate::app_context::AppContext;
use crate::command::shared::args::required_arg;
use crate::command::shared::wire::append_array_header;
use crate::config::model::ReplicationConfig;
use crate::resp::{append_bulk_response, append_integer_response, append_simple_response};

macro_rules! server_commands {
    ($m:ident) => {
        $m!(server, config, cmd_config);
        $m!(server, dbsize, cmd_dbsize);
        $m!(server, flushall, cmd_flushall);
        $m!(server, info, cmd_info);
        $m!(server, memory, cmd_memory);
        $m!(server, time, cmd_time);
    };
}
pub(crate) use server_commands;

const SERVER_VERSION: &str = env!("CARGO_PKG_VERSION");

fn format_size_for_config(bytes: u64) -> String {
    if bytes >= 1024 * 1024 * 1024 {
        format!("{}gb", bytes / (1024 * 1024 * 1024))
    } else if bytes >= 1024 * 1024 {
        format!("{}mb", bytes / (1024 * 1024))
    } else if bytes >= 1024 {
        format!("{}kb", bytes / 1024)
    } else {
        bytes.to_string()
    }
}

fn format_client_output_buffer_limit(hard: u64, soft: u64, soft_secs: u32) -> String {
    format!(
        "{} {} {}",
        format_size_for_config(hard),
        format_size_for_config(soft),
        soft_secs
    )
}

pub fn cmd_config(args: &[&[u8]], ctx: &AppContext, out: &mut Vec<u8>) -> Result<(), String> {
    let subcommand = required_arg(args, 0)?;
    if !subcommand.eq_ignore_ascii_case(b"GET") {
        return Err("ERR unknown subcommand or wrong number of arguments for 'config'".to_string());
    }
    let patterns = &args[1..];
    if patterns.is_empty() {
        return Err("ERR wrong number of arguments for 'config get' command".to_string());
    }
    let config = &ctx.config;
    let client_limits = &config.client_output_buffer_limits;
    let client_output_buffer_limit_value = format!(
        "normal {}\nreplica {}\npubsub {}",
        format_client_output_buffer_limit(
            client_limits.normal.hard_limit_bytes,
            client_limits.normal.soft_limit_bytes,
            client_limits.normal.soft_seconds,
        ),
        format_client_output_buffer_limit(
            client_limits.replica.hard_limit_bytes,
            client_limits.replica.soft_limit_bytes,
            client_limits.replica.soft_seconds,
        ),
        format_client_output_buffer_limit(
            client_limits.pubsub.hard_limit_bytes,
            client_limits.pubsub.soft_limit_bytes,
            client_limits.pubsub.soft_seconds,
        ),
    );
    let all_params: Vec<(&str, String)> = vec![
        ("port", config.port.to_string()),
        ("bind", config.bind.join(" ")),
        ("dir", config.rdb.dir.to_string_lossy().to_string()),
        ("dbfilename", config.rdb.dbfilename.clone()),
        (
            "stop-writes-on-bgsave-error",
            if config.rdb.stop_writes_on_bgsave_error {
                "yes"
            } else {
                "no"
            }
            .to_string(),
        ),
        (
            "rdbcompression",
            if config.rdb.rdbcompression {
                "yes"
            } else {
                "no"
            }
            .to_string(),
        ),
        (
            "rdbchecksum",
            if config.rdb.rdbchecksum { "yes" } else { "no" }.to_string(),
        ),
        (
            "rdb-save-incremental-fsync",
            if config.rdb.rdb_save_incremental_fsync {
                "yes"
            } else {
                "no"
            }
            .to_string(),
        ),
        (
            "appendonly",
            if config.aof.enabled { "yes" } else { "no" }.to_string(),
        ),
        ("appendfilename", config.aof.appendfilename.clone()),
        (
            "appendfsync",
            match config.aof.appendfsync {
                crate::config::model::AppendFsyncPolicy::Always => "always",
                crate::config::model::AppendFsyncPolicy::EverySec => "everysec",
                crate::config::model::AppendFsyncPolicy::No => "no",
            }
            .to_string(),
        ),
        (
            "auto-aof-rewrite-percentage",
            config.aof.auto_rewrite_percentage.to_string(),
        ),
        (
            "auto-aof-rewrite-min-size",
            format_size_for_config(config.aof.auto_rewrite_min_size_bytes),
        ),
        (
            "aof-use-rdb-preamble",
            if config.aof.use_rdb_preamble {
                "yes"
            } else {
                "no"
            }
            .to_string(),
        ),
        (
            "aof-rewrite-incremental-fsync",
            if config.aof.aof_rewrite_incremental_fsync {
                "yes"
            } else {
                "no"
            }
            .to_string(),
        ),
        (
            "requirepass",
            config.requirepass.clone().unwrap_or_default(),
        ),
        (
            "maxclients",
            config
                .maxclients
                .map(|n| n.to_string())
                .unwrap_or_else(|| "10000".to_string()),
        ),
        (
            "client-output-buffer-limit",
            client_output_buffer_limit_value,
        ),
        ("lua-time-limit", config.lua_time_limit.to_string()),
        ("hz", config.hz.to_string()),
        (
            "worker-threads",
            config
                .worker_threads
                .map(|n| n.to_string())
                .unwrap_or_else(|| "auto".to_string()),
        ),
        (
            "replicaof",
            match &config.replication {
                ReplicationConfig::Master => "no one".to_string(),
                ReplicationConfig::Replica { host, port } => format!("{host} {port}"),
            },
        ),
    ];
    let mut pairs: Vec<(&str, String)> = Vec::new();
    let want_all = patterns.iter().any(|p| *p == b"*");
    for (name, value) in &all_params {
        if want_all {
            pairs.push((*name, value.clone()));
        } else {
            for pattern in patterns {
                if std::str::from_utf8(pattern)
                    .map(|p| p.eq_ignore_ascii_case(name))
                    .unwrap_or(false)
                {
                    pairs.push((*name, value.clone()));
                    break;
                }
            }
        }
    }
    append_array_header(out, pairs.len() * 2);
    for (k, v) in pairs {
        append_bulk_response(out, Some(k.as_bytes()));
        append_bulk_response(out, Some(v.as_bytes()));
    }
    Ok(())
}

pub fn cmd_dbsize(_args: &[&[u8]], ctx: &AppContext, out: &mut Vec<u8>) -> Result<(), String> {
    let count = ctx.db.iter_live_keys().len() as i64;
    append_integer_response(out, count);
    Ok(())
}

pub fn cmd_flushall(_args: &[&[u8]], ctx: &AppContext, out: &mut Vec<u8>) -> Result<(), String> {
    if let Some(aof_engine) = ctx.aof.as_ref() {
        aof_engine
            .append_flushall()
            .map_err(|e| format!("ERR AOF append failed: {e}"))?;
    }
    ctx.db.flush_all();
    append_simple_response(out, "OK");
    Ok(())
}

pub fn cmd_info(args: &[&[u8]], ctx: &AppContext, out: &mut Vec<u8>) -> Result<(), String> {
    let section = args
        .first()
        .and_then(|v| std::str::from_utf8(v).ok())
        .unwrap_or("server");
    let mut buf = String::new();
    let keys_count = ctx.db.iter_live_keys().len();
    if section.eq_ignore_ascii_case("all")
        || section.eq_ignore_ascii_case("server")
        || section.eq_ignore_ascii_case("default")
    {
        buf.push_str("# Server\r\n");
        buf.push_str(&format!("redis_version:{}\r\n", SERVER_VERSION));
        let redis_mode = if ctx.config.is_replica() {
            "replica"
        } else {
            "standalone"
        };
        buf.push_str(&format!("redis_mode:{redis_mode}\r\n"));
    }
    if section.eq_ignore_ascii_case("all")
        || section.eq_ignore_ascii_case("clients")
        || section.eq_ignore_ascii_case("default")
    {
        buf.push_str("# Clients\r\n");
        buf.push_str("connected_clients:0\r\n");
        buf.push_str(&format!(
            "maxclients:{}\r\n",
            ctx.config.maxclients.unwrap_or(10000)
        ));
    }
    if section.eq_ignore_ascii_case("all")
        || section.eq_ignore_ascii_case("memory")
        || section.eq_ignore_ascii_case("default")
    {
        buf.push_str("# Memory\r\n");
        buf.push_str("used_memory:0\r\n");
        buf.push_str("used_memory_human:0B\r\n");
        buf.push_str(&format!("keys_count:{}\r\n", keys_count));
    }
    if section.eq_ignore_ascii_case("all")
        || section.eq_ignore_ascii_case("keyspace")
        || section.eq_ignore_ascii_case("default")
    {
        buf.push_str("# Keyspace\r\n");
        buf.push_str(&format!("db0:keys={}\r\n", keys_count));
    }
    if section.eq_ignore_ascii_case("all")
        || section.eq_ignore_ascii_case("replication")
        || section.eq_ignore_ascii_case("default")
    {
        let m = ctx.replication.replication_metrics();
        let role = if ctx.config.is_replica() {
            "slave"
        } else {
            "master"
        };
        buf.push_str("# Replication\r\n");
        buf.push_str(&format!("role:{role}\r\n"));
        buf.push_str(&format!("master_replid:{}\r\n", m.replid));
        buf.push_str(&format!("master_repl_offset:{}\r\n", m.master_offset));
        buf.push_str(&format!("replica_ack_offset:{}\r\n", m.last_ack_offset));
        buf.push_str(&format!("replica_lag_bytes:{}\r\n", m.lag_bytes));
        buf.push_str(&format!(
            "replica_ack_age_ms:{}\r\n",
            m.last_ack_age_ms
                .map(|v| v.to_string())
                .unwrap_or_else(|| "-1".to_string())
        ));
        buf.push_str(&format!("replica_ack_count:{}\r\n", m.ack_count));
        buf.push_str(&format!(
            "replication_capture_writes:{}\r\n",
            if m.capture_writes { 1 } else { 0 }
        ));
        buf.push_str(&format!(
            "replication_dropped_ingress_writes:{}\r\n",
            m.dropped_ingress_writes
        ));
    }
    if buf.is_empty() {
        buf.push_str(&format!("# {}\r\n\r\n", section));
    }
    append_bulk_response(out, Some(buf.as_bytes()));
    Ok(())
}

pub fn cmd_memory(args: &[&[u8]], ctx: &AppContext, out: &mut Vec<u8>) -> Result<(), String> {
    let subcommand = required_arg(args, 0)?;
    if subcommand.eq_ignore_ascii_case(b"STATS") {
        return cmd_memory_stats(ctx, out);
    }
    if subcommand.eq_ignore_ascii_case(b"USAGE") {
        return cmd_memory_usage(&args[1..], ctx, out);
    }
    Err("ERR unknown subcommand or wrong number of arguments for 'memory'".to_string())
}

fn cmd_memory_stats(ctx: &AppContext, out: &mut Vec<u8>) -> Result<(), String> {
    let keys = ctx.db.iter_live_keys();
    let keys_count = keys.len();
    let dataset_bytes = keys.iter().fold(0_usize, |acc, k| {
        acc + k.len() + ctx.db.get_entry(k).map(|e| e.value.len()).unwrap_or(0)
    });
    let bytes_per_key = if keys_count > 0 {
        dataset_bytes / keys_count
    } else {
        0
    };
    let total_allocated = dataset_bytes + keys_count * 64;
    let pairs: Vec<(&str, String)> = vec![
        ("keys.count", keys_count.to_string()),
        ("keys.bytes-per-key", bytes_per_key.to_string()),
        ("dataset.bytes", dataset_bytes.to_string()),
        ("total.allocated", total_allocated.to_string()),
        ("peak.allocated", total_allocated.to_string()),
        ("startup.allocated", "0".to_string()),
        ("overhead.total", "0".to_string()),
        ("fragmentation.bytes", "0".to_string()),
        ("fragmentation", "0".to_string()),
    ];
    append_array_header(out, pairs.len() * 2);
    for (k, v) in pairs {
        append_bulk_response(out, Some(k.as_bytes()));
        append_bulk_response(out, Some(v.as_bytes()));
    }
    Ok(())
}

fn cmd_memory_usage(args: &[&[u8]], ctx: &AppContext, out: &mut Vec<u8>) -> Result<(), String> {
    let key = required_arg(args, 0)?;
    let Some(entry) = ctx.db.get_entry(key) else {
        append_bulk_response(out, None);
        return Ok(());
    };
    const OVERHEAD: usize = 64;
    let usage = key.len() + entry.value.len() + OVERHEAD;
    append_integer_response(out, usage as i64);
    Ok(())
}

pub fn cmd_time(_args: &[&[u8]], _ctx: &AppContext, out: &mut Vec<u8>) -> Result<(), String> {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    let secs = now.as_secs();
    let micros = now.subsec_micros();
    append_array_header(out, 2);
    append_bulk_response(out, Some(secs.to_string().as_bytes()));
    append_bulk_response(out, Some(micros.to_string().as_bytes()));
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use bytes::Bytes;

    use crate::app_context::AppContext;
    use crate::config::default_config;
    use crate::replication::ReplicationManager;
    use crate::storage::{DashMapStorageEngine, DbConfig, StorageEngine, ValueEntry};

    use super::{
        cmd_dbsize, cmd_info, cmd_memory, format_client_output_buffer_limit, format_size_for_config,
    };

    fn test_ctx() -> AppContext {
        let db: Arc<dyn StorageEngine + Send + Sync> = Arc::new(
            DashMapStorageEngine::new(DbConfig { worker_count: 2 }).expect("db init should work"),
        );
        AppContext::new(
            default_config(),
            db,
            None,
            None,
            None,
            Arc::new(ReplicationManager::new()),
        )
    }

    #[test]
    fn format_size_for_config_uses_expected_units() {
        assert_eq!(format_size_for_config(0), "0");
        assert_eq!(format_size_for_config(1023), "1023");
        assert_eq!(format_size_for_config(1024), "1kb");
        assert_eq!(format_size_for_config(5 * 1024 * 1024), "5mb");
        assert_eq!(format_size_for_config(3 * 1024 * 1024 * 1024), "3gb");
    }

    #[test]
    fn format_client_output_buffer_limit_combines_three_fields() {
        let got = format_client_output_buffer_limit(2 * 1024, 4 * 1024 * 1024, 60);
        assert_eq!(got, "2kb 4mb 60");
    }

    #[test]
    fn dbsize_and_memory_usage_reflect_current_dataset() {
        let ctx = test_ctx();
        ctx.db.put_entry(
            b"k1",
            ValueEntry {
                value: Bytes::from_static(b"v1"),
                expire_at_ms: None,
            },
        );

        let mut out = Vec::new();
        cmd_dbsize(&[], &ctx, &mut out).expect("dbsize should succeed");
        assert_eq!(out, b":1\r\n");

        out.clear();
        cmd_memory(&[b"USAGE", b"k1"], &ctx, &mut out).expect("memory usage should succeed");
        assert_eq!(out, b":68\r\n");
    }

    #[test]
    fn info_server_and_all_sections_render_expected_blocks() {
        let ctx = test_ctx();
        let mut out = Vec::new();
        cmd_info(&[], &ctx, &mut out).expect("info should succeed");
        let server_only = String::from_utf8(out).expect("resp should be utf8");
        assert!(server_only.contains("# Server\r\n"));
        assert!(!server_only.contains("# Clients\r\n"));

        let mut out_all = Vec::new();
        cmd_info(&[b"all"], &ctx, &mut out_all).expect("info all should succeed");
        let all = String::from_utf8(out_all).expect("resp should be utf8");
        assert!(all.contains("# Server\r\n"));
        assert!(all.contains("# Clients\r\n"));
        assert!(all.contains("# Memory\r\n"));
        assert!(all.contains("# Keyspace\r\n"));
        assert!(all.contains("# Replication\r\n"));
    }
}
