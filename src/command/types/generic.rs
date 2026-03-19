use crate::app_context::AppContext;
use crate::command::shared::args::required_arg;
use crate::command::shared::time::current_time_ms;
use crate::command::shared::wire::{append_bulk_items, append_scan_response};
use crate::resp::{append_bulk_response, append_integer_response, append_simple_response};
use crate::resp::parse_ascii_u64;

macro_rules! generic_commands {
    ($m:ident) => {
        $m!(generic, del, cmd_del);
        $m!(generic, dump, cmd_dump);
        $m!(generic, exists, cmd_exists);
        $m!(generic, expire, cmd_expire);
        $m!(generic, expireat, cmd_expireat);
        $m!(generic, keys, cmd_keys);
        $m!(generic, persist, cmd_persist);
        $m!(generic, pexpire, cmd_pexpire);
        $m!(generic, pexpireat, cmd_pexpireat);
        $m!(generic, pttl, cmd_pttl);
        $m!(generic, scan, cmd_scan);
        $m!(generic, ttl, cmd_ttl);
        $m!(generic, r#type, cmd_type);
    };
}
pub(crate) use generic_commands;

const MAGIC: &[u8; 4] = b"FKV1";
const TYPE_HASH: u8 = 1;
const TYPE_LIST: u8 = 2;
const TYPE_SET: u8 = 3;
const TYPE_ZSET: u8 = 4;

pub fn cmd_del(args: &[&[u8]], ctx: &AppContext, out: &mut Vec<u8>) -> Result<(), String> {
    if args.is_empty() {
        return Err("ERR wrong number of arguments for 'del' command".to_string());
    }
    if let Some(aof_engine) = ctx.aof.as_ref() {
        aof_engine
            .append_del(args)
            .map_err(|e| format!("ERR AOF append failed: {e}"))?;
    }
    let mut removed = 0_i64;
    for key in args {
        if ctx.db.remove_entry(key) {
            removed += 1;
        }
    }
    append_integer_response(out, removed);
    Ok(())
}

pub fn cmd_dump(args: &[&[u8]], ctx: &AppContext, out: &mut Vec<u8>) -> Result<(), String> {
    let key = required_arg(args, 0)?;
    let value = ctx.db.get_entry(key).map(|e| e.value);
    append_bulk_response(out, value.as_deref());
    Ok(())
}

pub fn cmd_exists(args: &[&[u8]], ctx: &AppContext, out: &mut Vec<u8>) -> Result<(), String> {
    if args.is_empty() {
        return Err("ERR wrong number of arguments for 'exists' command".to_string());
    }
    let count = args
        .iter()
        .filter(|k| ctx.db.contains_live_key(k))
        .count() as i64;
    append_integer_response(out, count);
    Ok(())
}

pub fn cmd_expire(args: &[&[u8]], ctx: &AppContext, out: &mut Vec<u8>) -> Result<(), String> {
    let key = required_arg(args, 0)?;
    let seconds_raw = required_arg(args, 1)?;
    let seconds = parse_ttl_seconds(seconds_raw)?;
    let ttl_ms = seconds
        .checked_mul(1000)
        .ok_or_else(|| "ERR invalid expire time".to_string())?;
    set_expire(ctx, out, key, ttl_ms, |now| now.saturating_add(ttl_ms), b"EXPIRE", Some(seconds_raw))
}

pub fn cmd_expireat(args: &[&[u8]], ctx: &AppContext, out: &mut Vec<u8>) -> Result<(), String> {
    let key = required_arg(args, 0)?;
    let timestamp_raw = required_arg(args, 1)?;
    let timestamp_secs = parse_timestamp_seconds(timestamp_raw)?;
    let expire_at_ms = timestamp_secs
        .checked_mul(1000)
        .ok_or_else(|| "ERR value is not an integer or out of range".to_string())?;
    set_expire_at(ctx, out, key, expire_at_ms, b"EXPIREAT", Some(timestamp_raw))
}

pub fn cmd_keys(args: &[&[u8]], ctx: &AppContext, out: &mut Vec<u8>) -> Result<(), String> {
    let pattern = required_arg(args, 0)?;
    let keys = ctx.db.iter_live_keys();
    let matched: Vec<Vec<u8>> = keys
        .into_iter()
        .filter(|k| match_glob(k, pattern))
        .collect();
    append_bulk_items(out, &matched);
    Ok(())
}

pub fn cmd_persist(args: &[&[u8]], ctx: &AppContext, out: &mut Vec<u8>) -> Result<(), String> {
    let key = required_arg(args, 0)?;
    let Some(mut entry) = ctx.db.get_entry(key) else {
        append_integer_response(out, 0);
        return Ok(());
    };
    let had_ttl = entry.expire_at_ms.is_some();
    if had_ttl {
        if let Some(aof_engine) = ctx.aof.as_ref() {
            aof_engine
                .append_persist(key)
                .map_err(|e| format!("ERR AOF append failed: {e}"))?;
        }
        entry.expire_at_ms = None;
        ctx.db.put_entry(key, entry);
    }
    append_integer_response(out, if had_ttl { 1 } else { 0 });
    Ok(())
}

pub fn cmd_pexpire(args: &[&[u8]], ctx: &AppContext, out: &mut Vec<u8>) -> Result<(), String> {
    let key = required_arg(args, 0)?;
    let ms_raw = required_arg(args, 1)?;
    let ttl_ms = parse_ttl_ms(ms_raw)?;
    if ttl_ms == 0 {
        return Err("ERR invalid expire time".to_string());
    }
    set_expire(ctx, out, key, ttl_ms, |now| now.saturating_add(ttl_ms), b"PEXPIRE", Some(ms_raw))
}

pub fn cmd_pexpireat(args: &[&[u8]], ctx: &AppContext, out: &mut Vec<u8>) -> Result<(), String> {
    let key = required_arg(args, 0)?;
    let timestamp_ms_raw = required_arg(args, 1)?;
    let expire_at_ms = parse_timestamp_ms(timestamp_ms_raw)?;
    set_expire_at(ctx, out, key, expire_at_ms, b"PEXPIREAT", Some(timestamp_ms_raw))
}

pub fn cmd_pttl(args: &[&[u8]], ctx: &AppContext, out: &mut Vec<u8>) -> Result<(), String> {
    let key = required_arg(args, 0)?;
    let ttl = get_ttl_ms(ctx, key);
    append_integer_response(out, ttl);
    Ok(())
}

pub fn cmd_scan(args: &[&[u8]], ctx: &AppContext, out: &mut Vec<u8>) -> Result<(), String> {
    let cursor_raw = required_arg(args, 0)?;
    let cursor = parse_scan_cursor(cursor_raw)?;
    let mut pattern: Option<&[u8]> = None;
    let mut count = 10_usize;
    let mut i = 1_usize;
    while i < args.len() {
        let token = args[i];
        if token.eq_ignore_ascii_case(b"MATCH") {
            if i + 1 >= args.len() {
                return Err("ERR syntax error".to_string());
            }
            pattern = Some(args[i + 1]);
            i += 2;
            continue;
        }
        if token.eq_ignore_ascii_case(b"COUNT") {
            if i + 1 >= args.len() {
                return Err("ERR syntax error".to_string());
            }
            count = parse_ascii_u64(args[i + 1])
                .map_err(|_| "ERR value is not an integer or out of range".to_string())?
                .min(1000)
                .max(1) as usize;
            i += 2;
            continue;
        }
        return Err("ERR syntax error".to_string());
    }
    let (next_cursor, items) = ctx.db.scan_live_keys(cursor, count);
    let matched: Vec<Vec<u8>> = if let Some(pat) = pattern {
        items.into_iter().filter(|k| match_glob(k, pat)).collect()
    } else {
        items
    };
    append_scan_response(out, next_cursor, &matched);
    Ok(())
}

pub fn cmd_ttl(args: &[&[u8]], ctx: &AppContext, out: &mut Vec<u8>) -> Result<(), String> {
    let key = required_arg(args, 0)?;
    let ttl_ms = get_ttl_ms(ctx, key);
    let ttl_secs = if ttl_ms < 0 {
        ttl_ms
    } else {
        (ttl_ms / 1000) as i64
    };
    append_integer_response(out, ttl_secs);
    Ok(())
}

pub fn cmd_type(args: &[&[u8]], ctx: &AppContext, out: &mut Vec<u8>) -> Result<(), String> {
    let key = required_arg(args, 0)?;
    let type_name = match ctx.db.get_entry(key) {
        Some(entry) => value_type_name(&entry.value),
        None => "none",
    };
    append_simple_response(out, type_name);
    Ok(())
}

fn set_expire<F>(
    ctx: &AppContext,
    out: &mut Vec<u8>,
    key: &[u8],
    _ttl_ms: u64,
    expire_fn: F,
    cmd: &[u8],
    raw_arg: Option<&[u8]>,
) -> Result<(), String>
where
    F: FnOnce(u64) -> u64,
{
    let Some(mut entry) = ctx.db.get_entry(key) else {
        append_integer_response(out, 0);
        return Ok(());
    };
    if let Some(aof_engine) = ctx.aof.as_ref() {
        if let Some(arg) = raw_arg {
            match cmd {
                b"EXPIRE" => aof_engine.append_expire(key, arg).map_err(|e| format!("ERR AOF append failed: {e}"))?,
                b"PEXPIRE" => aof_engine.append_pexpire(key, arg).map_err(|e| format!("ERR AOF append failed: {e}"))?,
                _ => {}
            }
        }
    }
    let now = current_time_ms();
    entry.expire_at_ms = Some(expire_fn(now));
    ctx.db.put_entry(key, entry);
    append_integer_response(out, 1);
    Ok(())
}

fn set_expire_at(
    ctx: &AppContext,
    out: &mut Vec<u8>,
    key: &[u8],
    expire_at_ms: u64,
    cmd: &[u8],
    raw_arg: Option<&[u8]>,
) -> Result<(), String> {
    let Some(mut entry) = ctx.db.get_entry(key) else {
        append_integer_response(out, 0);
        return Ok(());
    };
    if let Some(aof_engine) = ctx.aof.as_ref() {
        if let Some(arg) = raw_arg {
            match cmd {
                b"EXPIREAT" => aof_engine.append_expireat(key, arg).map_err(|e| format!("ERR AOF append failed: {e}"))?,
                b"PEXPIREAT" => aof_engine.append_pexpireat(key, arg).map_err(|e| format!("ERR AOF append failed: {e}"))?,
                _ => {}
            }
        }
    }
    entry.expire_at_ms = Some(expire_at_ms);
    ctx.db.put_entry(key, entry);
    append_integer_response(out, 1);
    Ok(())
}

fn get_ttl_ms(ctx: &AppContext, key: &[u8]) -> i64 {
    let Some(entry) = ctx.db.get_entry(key) else {
        return -2;
    };
    let Some(expire_at) = entry.expire_at_ms else {
        return -1;
    };
    let now = current_time_ms();
    if expire_at <= now {
        return -2;
    }
    (expire_at - now) as i64
}

fn value_type_name(value: &[u8]) -> &'static str {
    if value.len() < 5 || &value[..4] != MAGIC {
        return "string";
    }
    match value[4] {
        TYPE_HASH => "hash",
        TYPE_LIST => "list",
        TYPE_SET => "set",
        TYPE_ZSET => "zset",
        _ => "string",
    }
}

fn parse_ttl_seconds(raw: &[u8]) -> Result<u64, String> {
    let v = parse_timestamp_seconds(raw)?;
    if v == 0 {
        return Err("ERR invalid expire time".to_string());
    }
    Ok(v)
}

fn parse_timestamp_seconds(raw: &[u8]) -> Result<u64, String> {
    let s = std::str::from_utf8(raw).map_err(|_| "ERR value is not an integer or out of range".to_string())?;
    let v: i64 = s.parse().map_err(|_| "ERR value is not an integer or out of range".to_string())?;
    Ok(if v < 0 { 0 } else { v as u64 })
}

fn parse_timestamp_ms(raw: &[u8]) -> Result<u64, String> {
    parse_ascii_u64(raw).map_err(|_| "ERR value is not an integer or out of range".to_string())
}

fn parse_ttl_ms(raw: &[u8]) -> Result<u64, String> {
    let v = parse_timestamp_ms(raw)?;
    if v == 0 {
        return Err("ERR invalid expire time".to_string());
    }
    Ok(v)
}

fn parse_scan_cursor(raw: &[u8]) -> Result<usize, String> {
    let n = parse_ascii_u64(raw).map_err(|_| "ERR invalid cursor".to_string())?;
    usize::try_from(n).map_err(|_| "ERR invalid cursor".to_string())
}

fn match_glob(key: &[u8], pattern: &[u8]) -> bool {
    match_glob_impl(key, 0, pattern, 0)
}

fn match_glob_impl(key: &[u8], ki: usize, pattern: &[u8], pi: usize) -> bool {
    if pi >= pattern.len() {
        return ki >= key.len();
    }
    match pattern[pi] {
        b'*' => {
            if pi + 1 >= pattern.len() {
                return true;
            }
            for k in ki..=key.len() {
                if match_glob_impl(key, k, pattern, pi + 1) {
                    return true;
                }
            }
            false
        }
        b'?' => {
            if ki >= key.len() {
                return false;
            }
            match_glob_impl(key, ki + 1, pattern, pi + 1)
        }
        b'\\' => {
            if pi + 1 >= pattern.len() {
                return false;
            }
            if ki >= key.len() || key[ki] != pattern[pi + 1] {
                return false;
            }
            match_glob_impl(key, ki + 1, pattern, pi + 2)
        }
        b'[' => {
            let end = pattern[pi + 1..].iter().position(|&b| b == b']');
            let Some(end) = end else {
                return false;
            };
            let end = pi + 1 + end;
            if ki >= key.len() {
                return false;
            }
            let c = key[ki];
            let mut neg = false;
            let mut i = pi + 1;
            if i < pattern.len() && pattern[i] == b'^' {
                neg = true;
                i += 1;
            }
            let mut matched = false;
            while i < end {
                if i + 2 <= end && pattern[i + 1] == b'-' {
                    let lo = pattern[i];
                    let hi = pattern[i + 2];
                    if c >= lo && c <= hi {
                        matched = true;
                    }
                    i += 3;
                } else {
                    if pattern[i] == c {
                        matched = true;
                    }
                    i += 1;
                }
            }
            if matched != neg {
                return false;
            }
            match_glob_impl(key, ki + 1, pattern, end + 1)
        }
        c => {
            if ki >= key.len() || key[ki] != c {
                return false;
            }
            match_glob_impl(key, ki + 1, pattern, pi + 1)
        }
    }
}
