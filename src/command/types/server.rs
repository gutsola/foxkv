use std::time::{SystemTime, UNIX_EPOCH};

use crate::app_context::AppContext;
use crate::command::shared::args::required_arg;
use crate::command::shared::wire::append_array_header;
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
    let all_params: Vec<(&str, String)> = vec![
        ("port", config.port.to_string()),
        ("bind", config.bind.join(" ")),
        ("dir", config.rdb.dir.to_string_lossy().to_string()),
        ("dbfilename", config.rdb.dbfilename.clone()),
        ("appendonly", if config.aof.enabled { "yes" } else { "no" }.to_string()),
        ("appendfilename", config.aof.appendfilename.clone()),
    ];
    let mut pairs: Vec<(&str, String)> = Vec::new();
    let want_all = patterns.iter().any(|p| *p == b"*");
    for (name, value) in &all_params {
        if want_all {
            pairs.push((*name, value.clone()));
        } else {
            for pattern in patterns {
                if std::str::from_utf8(pattern).map(|p| p.eq_ignore_ascii_case(name)).unwrap_or(false) {
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
        .get(0)
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
        buf.push_str("redis_mode:standalone\r\n");
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
        acc + k.len()
            + ctx
                .db
                .get_entry(k)
                .map(|e| e.value.len())
                .unwrap_or(0)
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
