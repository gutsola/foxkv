use std::collections::BTreeMap;

use crate::app_context::AppContext;
use crate::command::shared::args::required_arg;
use crate::command::shared::typed_value::{
    TypedValue, decode_value, encode_hash, wrong_type_error,
};
use crate::command::shared::wire::{append_array_header, append_bool_integer, append_bulk_items};
use crate::resp::{append_bulk_response, append_integer_response, append_simple_response};
use crate::storage::ValueEntry;

macro_rules! hash_commands {
    ($m:ident) => {
        $m!(hash, hdel, cmd_hdel);
        $m!(hash, hexists, cmd_hexists);
        $m!(hash, hget, cmd_hget);
        $m!(hash, hgetall, cmd_hgetall);
        $m!(hash, hincrby, cmd_hincrby);
        $m!(hash, hincrbyfloat, cmd_hincrbyfloat);
        $m!(hash, hkeys, cmd_hkeys);
        $m!(hash, hlen, cmd_hlen);
        $m!(hash, hmget, cmd_hmget);
        $m!(hash, hmset, cmd_hmset);
        $m!(hash, hscan, cmd_hscan);
        $m!(hash, hset, cmd_hset);
        $m!(hash, hsetnx, cmd_hsetnx);
        $m!(hash, hstrlen, cmd_hstrlen);
        $m!(hash, hvals, cmd_hvals);
    };
}
pub(crate) use hash_commands;

pub fn cmd_hdel(args: &[&[u8]], ctx: &AppContext, out: &mut Vec<u8>) -> Result<(), String> {
    let key = required_arg(args, 0)?;
    if args.len() < 2 {
        return Err("ERR wrong number of arguments for 'hdel' command".to_string());
    }
    let Some(mut map) = get_hash(ctx, key)? else {
        append_integer_response(out, 0);
        return Ok(());
    };
    let mut removed = 0_i64;
    for field in &args[1..] {
        if map.remove(*field).is_some() {
            removed += 1;
        }
    }
    persist_hash_or_delete(ctx, key, map);
    append_integer_response(out, removed);
    Ok(())
}

pub fn cmd_hexists(args: &[&[u8]], ctx: &AppContext, out: &mut Vec<u8>) -> Result<(), String> {
    let key = required_arg(args, 0)?;
    let field = required_arg(args, 1)?;
    let exists = get_hash(ctx, key)?
        .map(|map| map.contains_key(field))
        .unwrap_or(false);
    append_bool_integer(out, exists);
    Ok(())
}

pub fn cmd_hget(args: &[&[u8]], ctx: &AppContext, out: &mut Vec<u8>) -> Result<(), String> {
    let key = required_arg(args, 0)?;
    let field = required_arg(args, 1)?;
    let value = get_hash(ctx, key)?.and_then(|map| map.get(field).cloned());
    append_bulk_response(out, value.as_deref());
    Ok(())
}

pub fn cmd_hgetall(args: &[&[u8]], ctx: &AppContext, out: &mut Vec<u8>) -> Result<(), String> {
    let key = required_arg(args, 0)?;
    let Some(map) = get_hash(ctx, key)? else {
        append_array_header(out, 0);
        return Ok(());
    };
    append_array_header(out, map.len() * 2);
    for (field, value) in map {
        append_bulk_response(out, Some(&field));
        append_bulk_response(out, Some(&value));
    }
    Ok(())
}

pub fn cmd_hincrby(args: &[&[u8]], ctx: &AppContext, out: &mut Vec<u8>) -> Result<(), String> {
    let key = required_arg(args, 0)?;
    let field = required_arg(args, 1)?;
    let delta = parse_i64(required_arg(args, 2)?)?;
    let mut map = get_hash(ctx, key)?.unwrap_or_default();
    let current = map.get(field).map(|v| parse_i64(v)).transpose()?.unwrap_or(0);
    let next = current
        .checked_add(delta)
        .ok_or_else(|| "ERR value is not an integer or out of range".to_string())?;
    map.insert(field.to_vec(), next.to_string().into_bytes());
    persist_hash(ctx, key, map);
    append_integer_response(out, next);
    Ok(())
}

pub fn cmd_hincrbyfloat(
    args: &[&[u8]],
    ctx: &AppContext,
    out: &mut Vec<u8>,
) -> Result<(), String> {
    let key = required_arg(args, 0)?;
    let field = required_arg(args, 1)?;
    let delta = parse_f64(required_arg(args, 2)?)?;
    let mut map = get_hash(ctx, key)?.unwrap_or_default();
    let current = map.get(field).map(|v| parse_f64(v)).transpose()?.unwrap_or(0.0);
    let next = current + delta;
    if !next.is_finite() {
        return Err("ERR increment would produce NaN or Infinity".to_string());
    }
    let next_bytes = next.to_string().into_bytes();
    map.insert(field.to_vec(), next_bytes.clone());
    persist_hash(ctx, key, map);
    append_bulk_response(out, Some(&next_bytes));
    Ok(())
}

pub fn cmd_hkeys(args: &[&[u8]], ctx: &AppContext, out: &mut Vec<u8>) -> Result<(), String> {
    let key = required_arg(args, 0)?;
    let keys = get_hash(ctx, key)?
        .map(|map| map.into_keys().collect::<Vec<Vec<u8>>>())
        .unwrap_or_default();
    append_bulk_items(out, &keys);
    Ok(())
}

pub fn cmd_hlen(args: &[&[u8]], ctx: &AppContext, out: &mut Vec<u8>) -> Result<(), String> {
    let key = required_arg(args, 0)?;
    let len = get_hash(ctx, key)?.map(|map| map.len()).unwrap_or(0);
    append_integer_response(out, len as i64);
    Ok(())
}

pub fn cmd_hmget(args: &[&[u8]], ctx: &AppContext, out: &mut Vec<u8>) -> Result<(), String> {
    let key = required_arg(args, 0)?;
    if args.len() < 2 {
        return Err("ERR wrong number of arguments for 'hmget' command".to_string());
    }
    let map = get_hash(ctx, key)?.unwrap_or_default();
    append_array_header(out, args.len() - 1);
    for field in &args[1..] {
        let value = map.get(*field);
        append_bulk_response(out, value.map(|v| v.as_slice()));
    }
    Ok(())
}

pub fn cmd_hmset(args: &[&[u8]], ctx: &AppContext, out: &mut Vec<u8>) -> Result<(), String> {
    let key = required_arg(args, 0)?;
    if args.len() < 3 || args.len() % 2 == 0 {
        return Err("ERR wrong number of arguments for 'hmset' command".to_string());
    }
    let mut map = get_hash(ctx, key)?.unwrap_or_default();
    let mut idx = 1_usize;
    while idx < args.len() {
        map.insert(args[idx].to_vec(), args[idx + 1].to_vec());
        idx += 2;
    }
    persist_hash(ctx, key, map);
    append_simple_response(out, "OK");
    Ok(())
}

pub fn cmd_hscan(args: &[&[u8]], ctx: &AppContext, out: &mut Vec<u8>) -> Result<(), String> {
    let key = required_arg(args, 0)?;
    let cursor = parse_usize(required_arg(args, 1)?)?;
    if cursor != 0 {
        append_array_header(out, 2);
        append_bulk_response(out, Some(b"0"));
        append_array_header(out, 0);
        return Ok(());
    }
    let map = get_hash(ctx, key)?.unwrap_or_default();
    let mut flat = Vec::with_capacity(map.len() * 2);
    for (field, value) in map {
        flat.push(field);
        flat.push(value);
    }
    append_array_header(out, 2);
    append_bulk_response(out, Some(b"0"));
    append_bulk_items(out, &flat);
    Ok(())
}

pub fn cmd_hset(args: &[&[u8]], ctx: &AppContext, out: &mut Vec<u8>) -> Result<(), String> {
    let key = required_arg(args, 0)?;
    if args.len() < 3 || args.len() % 2 == 0 {
        return Err("ERR wrong number of arguments for 'hset' command".to_string());
    }
    let mut map = get_hash(ctx, key)?.unwrap_or_default();
    let mut added = 0_i64;
    let mut idx = 1_usize;
    while idx < args.len() {
        let field = args[idx].to_vec();
        let value = args[idx + 1].to_vec();
        if map.insert(field, value).is_none() {
            added += 1;
        }
        idx += 2;
    }
    persist_hash(ctx, key, map);
    append_integer_response(out, added);
    Ok(())
}

pub fn cmd_hsetnx(args: &[&[u8]], ctx: &AppContext, out: &mut Vec<u8>) -> Result<(), String> {
    let key = required_arg(args, 0)?;
    let field = required_arg(args, 1)?;
    let value = required_arg(args, 2)?;
    let mut map = get_hash(ctx, key)?.unwrap_or_default();
    if map.contains_key(field) {
        append_integer_response(out, 0);
    } else {
        map.insert(field.to_vec(), value.to_vec());
        persist_hash(ctx, key, map);
        append_integer_response(out, 1);
    }
    Ok(())
}

pub fn cmd_hstrlen(args: &[&[u8]], ctx: &AppContext, out: &mut Vec<u8>) -> Result<(), String> {
    let key = required_arg(args, 0)?;
    let field = required_arg(args, 1)?;
    let len = get_hash(ctx, key)?
        .and_then(|map| map.get(field).map(|v| v.len()))
        .unwrap_or(0);
    append_integer_response(out, len as i64);
    Ok(())
}

pub fn cmd_hvals(args: &[&[u8]], ctx: &AppContext, out: &mut Vec<u8>) -> Result<(), String> {
    let key = required_arg(args, 0)?;
    let values = get_hash(ctx, key)?
        .map(|map| map.into_values().collect::<Vec<Vec<u8>>>())
        .unwrap_or_default();
    append_bulk_items(out, &values);
    Ok(())
}

fn get_hash(ctx: &AppContext, key: &[u8]) -> Result<Option<BTreeMap<Vec<u8>, Vec<u8>>>, String> {
    let Some(entry) = ctx.db.get_entry(key) else {
        return Ok(None);
    };
    match decode_value(&entry.value)? {
        TypedValue::Hash(map) => Ok(Some(map)),
        TypedValue::String(_) => Err(wrong_type_error()),
        _ => Err(wrong_type_error()),
    }
}

fn persist_hash(ctx: &AppContext, key: &[u8], map: BTreeMap<Vec<u8>, Vec<u8>>) {
    let value = encode_hash(&map);
    ctx.db.put_entry(
        key,
        ValueEntry {
            value,
            expire_at_ms: None,
        },
    );
}

fn persist_hash_or_delete(ctx: &AppContext, key: &[u8], map: BTreeMap<Vec<u8>, Vec<u8>>) {
    if map.is_empty() {
        let _ = ctx.db.remove_entry(key);
    } else {
        persist_hash(ctx, key, map);
    }
}

fn parse_i64(raw: &[u8]) -> Result<i64, String> {
    std::str::from_utf8(raw)
        .map_err(|_| "ERR value is not an integer or out of range".to_string())?
        .parse::<i64>()
        .map_err(|_| "ERR value is not an integer or out of range".to_string())
}

fn parse_f64(raw: &[u8]) -> Result<f64, String> {
    let value = std::str::from_utf8(raw)
        .map_err(|_| "ERR value is not a valid float".to_string())?
        .parse::<f64>()
        .map_err(|_| "ERR value is not a valid float".to_string())?;
    if !value.is_finite() {
        return Err("ERR value is not a valid float".to_string());
    }
    Ok(value)
}

fn parse_usize(raw: &[u8]) -> Result<usize, String> {
    std::str::from_utf8(raw)
        .map_err(|_| "ERR invalid cursor".to_string())?
        .parse::<usize>()
        .map_err(|_| "ERR invalid cursor".to_string())
}

