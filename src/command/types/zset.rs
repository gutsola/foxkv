use bytes::Bytes;

use crate::app_context::AppContext;
use crate::command::shared::args::required_arg;
use crate::command::shared::typed_value::{
    TypedValue, decode_value, encode_zset, wrong_type_error,
};
use crate::command::shared::wire::{append_array_header, append_bulk_items};
use crate::command::shared::zset::ZSet;
use crate::resp::{append_bulk_response, append_integer_response};
use crate::storage::ValueEntry;

macro_rules! zset_commands {
    ($m:ident) => {
        $m!(zset, bzpopmax, cmd_bzpopmax);
        $m!(zset, bzpopmin, cmd_bzpopmin);
        $m!(zset, zadd, cmd_zadd);
        $m!(zset, zcard, cmd_zcard);
        $m!(zset, zcount, cmd_zcount);
        $m!(zset, zincrby, cmd_zincrby);
        $m!(zset, zinterstore, cmd_zinterstore);
        $m!(zset, zlexcount, cmd_zlexcount);
        $m!(zset, zpopmax, cmd_zpopmax);
        $m!(zset, zpopmin, cmd_zpopmin);
        $m!(zset, zrange, cmd_zrange);
        $m!(zset, zrangebylex, cmd_zrangebylex);
        $m!(zset, zrangebyscore, cmd_zrangebyscore);
        $m!(zset, zrank, cmd_zrank);
        $m!(zset, zrem, cmd_zrem);
        $m!(zset, zremrangebylex, cmd_zremrangebylex);
        $m!(zset, zremrangebyrank, cmd_zremrangebyrank);
        $m!(zset, zremrangebyscore, cmd_zremrangebyscore);
        $m!(zset, zrevrange, cmd_zrevrange);
        $m!(zset, zrevrangebylex, cmd_zrevrangebylex);
        $m!(zset, zrevrangebyscore, cmd_zrevrangebyscore);
        $m!(zset, zrevrank, cmd_zrevrank);
        $m!(zset, zscan, cmd_zscan);
        $m!(zset, zscore, cmd_zscore);
        $m!(zset, zunionstore, cmd_zunionstore);
    };
}
pub(crate) use zset_commands;

pub fn cmd_bzpopmax(args: &[&[u8]], ctx: &AppContext, out: &mut Vec<u8>) -> Result<(), String> {
    if args.len() < 2 {
        return Err("ERR wrong number of arguments for 'bzpopmax' command".to_string());
    }
    for key in &args[..args.len() - 1] {
        if let Some(mut zset) = get_zset(ctx, key)? {
            if let Some((m, s)) = zset.pop_max() {
                persist_zset_or_delete(ctx, key, zset);
                append_array_header(out, 3);
                append_bulk_response(out, Some(key));
                append_bulk_response(out, Some(&m));
                append_bulk_response(out, Some(s.to_string().as_bytes()));
                return Ok(());
            }
        }
    }
    append_bulk_response(out, None);
    Ok(())
}

pub fn cmd_bzpopmin(args: &[&[u8]], ctx: &AppContext, out: &mut Vec<u8>) -> Result<(), String> {
    if args.len() < 2 {
        return Err("ERR wrong number of arguments for 'bzpopmin' command".to_string());
    }
    for key in &args[..args.len() - 1] {
        if let Some(mut zset) = get_zset(ctx, key)? {
            if let Some((m, s)) = zset.pop_min() {
                persist_zset_or_delete(ctx, key, zset);
                append_array_header(out, 3);
                append_bulk_response(out, Some(key));
                append_bulk_response(out, Some(&m));
                append_bulk_response(out, Some(s.to_string().as_bytes()));
                return Ok(());
            }
        }
    }
    append_bulk_response(out, None);
    Ok(())
}

pub fn cmd_zadd(args: &[&[u8]], ctx: &AppContext, out: &mut Vec<u8>) -> Result<(), String> {
    let key = required_arg(args, 0)?;
    if args.len() < 3 || (args.len() - 1) % 2 != 0 {
        return Err("ERR wrong number of arguments for 'zadd' command".to_string());
    }
    let mut zset = get_zset(ctx, key)?.unwrap_or_default();
    let mut added = 0_i64;
    let mut idx = 1_usize;
    while idx < args.len() {
        let score = parse_score(required_arg(args, idx)?)?;
        let member = required_arg(args, idx + 1)?;
        if zset.add(member.to_vec(), score)? {
            added += 1;
        }
        idx += 2;
    }
    persist_zset(ctx, key, zset);
    append_integer_response(out, added);
    Ok(())
}

pub fn cmd_zcard(args: &[&[u8]], ctx: &AppContext, out: &mut Vec<u8>) -> Result<(), String> {
    let key = required_arg(args, 0)?;
    let len = get_zset(ctx, key)?.map(|z| z.len()).unwrap_or(0);
    append_integer_response(out, len as i64);
    Ok(())
}

pub fn cmd_zcount(args: &[&[u8]], ctx: &AppContext, out: &mut Vec<u8>) -> Result<(), String> {
    let key = required_arg(args, 0)?;
    let min_spec = required_arg(args, 1)?;
    let max_spec = required_arg(args, 2)?;
    let (min, min_excl) = parse_score_bound(min_spec)?;
    let (max, max_excl) = parse_score_bound(max_spec)?;
    let count = get_zset(ctx, key)?
        .map(|z| z.count_by_score(min, min_excl, max, max_excl))
        .unwrap_or(0);
    append_integer_response(out, count as i64);
    Ok(())
}

pub fn cmd_zincrby(args: &[&[u8]], ctx: &AppContext, out: &mut Vec<u8>) -> Result<(), String> {
    let key = required_arg(args, 0)?;
    let delta = parse_score(required_arg(args, 1)?)?;
    let member = required_arg(args, 2)?;
    let mut zset = get_zset(ctx, key)?.unwrap_or_default();
    let new_score = zset.incr_by(member, delta)?;
    persist_zset_or_delete(ctx, key, zset);
    append_bulk_response(out, Some(new_score.to_string().as_bytes()));
    Ok(())
}

pub fn cmd_zinterstore(args: &[&[u8]], ctx: &AppContext, out: &mut Vec<u8>) -> Result<(), String> {
    let dest = required_arg(args, 0)?;
    let num_keys = parse_usize(required_arg(args, 1)?)?;
    if args.len() < 2 + num_keys {
        return Err("ERR syntax error".to_string());
    }
    let keys: Vec<&[u8]> = args[2..2 + num_keys].to_vec();
    let result = compute_zinter(ctx, &keys)?;
    let len = result.len() as i64;
    persist_zset_or_delete(ctx, dest, result);
    append_integer_response(out, len);
    Ok(())
}

pub fn cmd_zlexcount(args: &[&[u8]], ctx: &AppContext, out: &mut Vec<u8>) -> Result<(), String> {
    let key = required_arg(args, 0)?;
    let (min, min_inc) = parse_lex_bound(required_arg(args, 1)?)?;
    let (max, max_inc) = parse_lex_bound(required_arg(args, 2)?)?;
    let count = get_zset(ctx, key)?
        .map(|z| z.count_by_lex(&min, min_inc, &max, max_inc))
        .unwrap_or(0);
    append_integer_response(out, count as i64);
    Ok(())
}

pub fn cmd_zpopmax(args: &[&[u8]], ctx: &AppContext, out: &mut Vec<u8>) -> Result<(), String> {
    let key = required_arg(args, 0)?;
    let count = if args.len() > 1 {
        parse_usize(args[1])?.max(1)
    } else {
        1
    };
    let Some(mut zset) = get_zset(ctx, key)? else {
        if count > 1 {
            append_array_header(out, 0);
        } else {
            append_bulk_response(out, None);
        }
        return Ok(());
    };
    let mut popped = Vec::new();
    for _ in 0..count {
        if let Some((m, s)) = zset.pop_max() {
            popped.push((m, s));
        } else {
            break;
        }
    }
    persist_zset_or_delete(ctx, key, zset);
    if count == 1 {
        if let Some((m, s)) = popped.first() {
            append_array_header(out, 2);
            append_bulk_response(out, Some(m));
            append_bulk_response(out, Some(s.to_string().as_bytes()));
        } else {
            append_bulk_response(out, None);
        }
    } else {
        let flat: Vec<Vec<u8>> = popped
            .into_iter()
            .flat_map(|(m, s)| vec![m, s.to_string().into_bytes()])
            .collect();
        append_bulk_items(out, &flat);
    }
    Ok(())
}

pub fn cmd_zpopmin(args: &[&[u8]], ctx: &AppContext, out: &mut Vec<u8>) -> Result<(), String> {
    let key = required_arg(args, 0)?;
    let count = if args.len() > 1 {
        parse_usize(args[1])?.max(1)
    } else {
        1
    };
    let Some(mut zset) = get_zset(ctx, key)? else {
        if count > 1 {
            append_array_header(out, 0);
        } else {
            append_bulk_response(out, None);
        }
        return Ok(());
    };
    let mut popped = Vec::new();
    for _ in 0..count {
        if let Some((m, s)) = zset.pop_min() {
            popped.push((m, s));
        } else {
            break;
        }
    }
    persist_zset_or_delete(ctx, key, zset);
    if count == 1 {
        if let Some((m, s)) = popped.first() {
            append_array_header(out, 2);
            append_bulk_response(out, Some(m));
            append_bulk_response(out, Some(s.to_string().as_bytes()));
        } else {
            append_bulk_response(out, None);
        }
    } else {
        let flat: Vec<Vec<u8>> = popped
            .into_iter()
            .flat_map(|(m, s)| vec![m, s.to_string().into_bytes()])
            .collect();
        append_bulk_items(out, &flat);
    }
    Ok(())
}

pub fn cmd_zrange(args: &[&[u8]], ctx: &AppContext, out: &mut Vec<u8>) -> Result<(), String> {
    let key = required_arg(args, 0)?;
    let start = parse_i64(required_arg(args, 1)?)?;
    let stop = parse_i64(required_arg(args, 2)?)?;
    let withscores = args.len() > 3 && args[3].eq_ignore_ascii_case(b"withscores");
    let Some(zset) = get_zset(ctx, key)? else {
        append_array_header(out, 0);
        return Ok(());
    };
    let range = zset.range_by_rank(start, stop);
    if withscores {
        let flat: Vec<Vec<u8>> = range
            .into_iter()
            .flat_map(|(m, s)| vec![m, s.to_string().into_bytes()])
            .collect();
        append_bulk_items(out, &flat);
    } else {
        let members: Vec<Vec<u8>> = range.into_iter().map(|(m, _)| m).collect();
        append_bulk_items(out, &members);
    }
    Ok(())
}

pub fn cmd_zrangebylex(args: &[&[u8]], ctx: &AppContext, out: &mut Vec<u8>) -> Result<(), String> {
    let key = required_arg(args, 0)?;
    let (min, min_inc) = parse_lex_bound(required_arg(args, 1)?)?;
    let (max, max_inc) = parse_lex_bound(required_arg(args, 2)?)?;
    let members = get_zset(ctx, key)?
        .map(|z| z.range_by_lex(&min, min_inc, &max, max_inc))
        .unwrap_or_default();
    append_bulk_items(out, &members);
    Ok(())
}

pub fn cmd_zrangebyscore(
    args: &[&[u8]],
    ctx: &AppContext,
    out: &mut Vec<u8>,
) -> Result<(), String> {
    let key = required_arg(args, 0)?;
    let (min, min_excl) = parse_score_bound(required_arg(args, 1)?)?;
    let (max, max_excl) = parse_score_bound(required_arg(args, 2)?)?;
    let withscores = args.len() > 3 && args[3].eq_ignore_ascii_case(b"withscores");
    let Some(zset) = get_zset(ctx, key)? else {
        append_array_header(out, 0);
        return Ok(());
    };
    let range = zset.range_by_score(min, min_excl, max, max_excl);
    if withscores {
        let flat: Vec<Vec<u8>> = range
            .into_iter()
            .flat_map(|(m, s)| vec![m, s.to_string().into_bytes()])
            .collect();
        append_bulk_items(out, &flat);
    } else {
        let members: Vec<Vec<u8>> = range.into_iter().map(|(m, _)| m).collect();
        append_bulk_items(out, &members);
    }
    Ok(())
}

pub fn cmd_zrank(args: &[&[u8]], ctx: &AppContext, out: &mut Vec<u8>) -> Result<(), String> {
    let key = required_arg(args, 0)?;
    let member = required_arg(args, 1)?;
    let rank = get_zset(ctx, key)?.and_then(|z| z.rank(member));
    match rank {
        Some(r) => append_integer_response(out, r as i64),
        None => append_bulk_response(out, None),
    }
    Ok(())
}

pub fn cmd_zrem(args: &[&[u8]], ctx: &AppContext, out: &mut Vec<u8>) -> Result<(), String> {
    let key = required_arg(args, 0)?;
    if args.len() < 2 {
        return Err("ERR wrong number of arguments for 'zrem' command".to_string());
    }
    let Some(mut zset) = get_zset(ctx, key)? else {
        append_integer_response(out, 0);
        return Ok(());
    };
    let mut removed = 0_i64;
    for member in &args[1..] {
        if zset.remove(*member) {
            removed += 1;
        }
    }
    persist_zset_or_delete(ctx, key, zset);
    append_integer_response(out, removed);
    Ok(())
}

pub fn cmd_zremrangebylex(
    args: &[&[u8]],
    ctx: &AppContext,
    out: &mut Vec<u8>,
) -> Result<(), String> {
    let key = required_arg(args, 0)?;
    let (min, min_inc) = parse_lex_bound(required_arg(args, 1)?)?;
    let (max, max_inc) = parse_lex_bound(required_arg(args, 2)?)?;
    let Some(mut zset) = get_zset(ctx, key)? else {
        append_integer_response(out, 0);
        return Ok(());
    };
    let removed = zset.remove_by_lex(&min, min_inc, &max, max_inc);
    persist_zset_or_delete(ctx, key, zset);
    append_integer_response(out, removed as i64);
    Ok(())
}

pub fn cmd_zremrangebyrank(
    args: &[&[u8]],
    ctx: &AppContext,
    out: &mut Vec<u8>,
) -> Result<(), String> {
    let key = required_arg(args, 0)?;
    let start = parse_i64(required_arg(args, 1)?)?;
    let stop = parse_i64(required_arg(args, 2)?)?;
    let Some(mut zset) = get_zset(ctx, key)? else {
        append_integer_response(out, 0);
        return Ok(());
    };
    let removed = zset.remove_by_rank(start, stop);
    persist_zset_or_delete(ctx, key, zset);
    append_integer_response(out, removed as i64);
    Ok(())
}

pub fn cmd_zremrangebyscore(
    args: &[&[u8]],
    ctx: &AppContext,
    out: &mut Vec<u8>,
) -> Result<(), String> {
    let key = required_arg(args, 0)?;
    let (min, min_excl) = parse_score_bound(required_arg(args, 1)?)?;
    let (max, max_excl) = parse_score_bound(required_arg(args, 2)?)?;
    let Some(mut zset) = get_zset(ctx, key)? else {
        append_integer_response(out, 0);
        return Ok(());
    };
    let removed = zset.remove_by_score(min, min_excl, max, max_excl);
    persist_zset_or_delete(ctx, key, zset);
    append_integer_response(out, removed as i64);
    Ok(())
}

pub fn cmd_zrevrange(args: &[&[u8]], ctx: &AppContext, out: &mut Vec<u8>) -> Result<(), String> {
    let key = required_arg(args, 0)?;
    let start = parse_i64(required_arg(args, 1)?)?;
    let stop = parse_i64(required_arg(args, 2)?)?;
    let withscores = args.len() > 3 && args[3].eq_ignore_ascii_case(b"withscores");
    let Some(zset) = get_zset(ctx, key)? else {
        append_array_header(out, 0);
        return Ok(());
    };
    let range = zset.range_by_rank_rev(start, stop);
    if withscores {
        let flat: Vec<Vec<u8>> = range
            .into_iter()
            .flat_map(|(m, s)| vec![m, s.to_string().into_bytes()])
            .collect();
        append_bulk_items(out, &flat);
    } else {
        let members: Vec<Vec<u8>> = range.into_iter().map(|(m, _)| m).collect();
        append_bulk_items(out, &members);
    }
    Ok(())
}

pub fn cmd_zrevrangebylex(
    args: &[&[u8]],
    ctx: &AppContext,
    out: &mut Vec<u8>,
) -> Result<(), String> {
    let key = required_arg(args, 0)?;
    let (max, max_inc) = parse_lex_bound(required_arg(args, 1)?)?;
    let (min, min_inc) = parse_lex_bound(required_arg(args, 2)?)?;
    let members = get_zset(ctx, key)?
        .map(|z| z.range_by_lex_rev(&min, min_inc, &max, max_inc))
        .unwrap_or_default();
    append_bulk_items(out, &members);
    Ok(())
}

pub fn cmd_zrevrangebyscore(
    args: &[&[u8]],
    ctx: &AppContext,
    out: &mut Vec<u8>,
) -> Result<(), String> {
    let key = required_arg(args, 0)?;
    let (max, max_excl) = parse_score_bound(required_arg(args, 1)?)?;
    let (min, min_excl) = parse_score_bound(required_arg(args, 2)?)?;
    let mut withscores = false;
    if args.len() > 3 && args[3].eq_ignore_ascii_case(b"withscores") {
        withscores = true;
    }
    let Some(zset) = get_zset(ctx, key)? else {
        append_array_header(out, 0);
        return Ok(());
    };
    let range = zset.range_by_score_rev(min, min_excl, max, max_excl);
    if withscores {
        let flat: Vec<Vec<u8>> = range
            .into_iter()
            .flat_map(|(m, s)| vec![m, s.to_string().into_bytes()])
            .collect();
        append_bulk_items(out, &flat);
    } else {
        let members: Vec<Vec<u8>> = range.into_iter().map(|(m, _)| m).collect();
        append_bulk_items(out, &members);
    }
    Ok(())
}

pub fn cmd_zrevrank(args: &[&[u8]], ctx: &AppContext, out: &mut Vec<u8>) -> Result<(), String> {
    let key = required_arg(args, 0)?;
    let member = required_arg(args, 1)?;
    let rank = get_zset(ctx, key)?.and_then(|z| z.rev_rank(member));
    match rank {
        Some(r) => append_integer_response(out, r as i64),
        None => append_bulk_response(out, None),
    }
    Ok(())
}

pub fn cmd_zscan(args: &[&[u8]], ctx: &AppContext, out: &mut Vec<u8>) -> Result<(), String> {
    let key = required_arg(args, 0)?;
    let cursor = parse_usize(required_arg(args, 1)?)?;
    if cursor != 0 {
        append_array_header(out, 2);
        append_bulk_response(out, Some(b"0"));
        append_array_header(out, 0);
        return Ok(());
    }
    let zset = get_zset(ctx, key)?.unwrap_or_default();
    let flat: Vec<Vec<u8>> = zset
        .iter()
        .flat_map(|(m, s)| vec![m, s.to_string().into_bytes()])
        .collect();
    append_array_header(out, 2);
    append_bulk_response(out, Some(b"0"));
    append_bulk_items(out, &flat);
    Ok(())
}

pub fn cmd_zscore(args: &[&[u8]], ctx: &AppContext, out: &mut Vec<u8>) -> Result<(), String> {
    let key = required_arg(args, 0)?;
    let member = required_arg(args, 1)?;
    let score = get_zset(ctx, key)?.and_then(|z| z.score(member));
    match score {
        Some(s) => append_bulk_response(out, Some(s.to_string().as_bytes())),
        None => append_bulk_response(out, None),
    }
    Ok(())
}

pub fn cmd_zunionstore(args: &[&[u8]], ctx: &AppContext, out: &mut Vec<u8>) -> Result<(), String> {
    let dest = required_arg(args, 0)?;
    let num_keys = parse_usize(required_arg(args, 1)?)?;
    if args.len() < 2 + num_keys {
        return Err("ERR syntax error".to_string());
    }
    let keys: Vec<&[u8]> = args[2..2 + num_keys].to_vec();
    let result = compute_zunion(ctx, &keys)?;
    let len = result.len() as i64;
    persist_zset_or_delete(ctx, dest, result);
    append_integer_response(out, len);
    Ok(())
}

fn get_zset(ctx: &AppContext, key: &[u8]) -> Result<Option<ZSet>, String> {
    let Some(entry) = ctx.db.get_entry(key) else {
        return Ok(None);
    };
    match decode_value(&entry.value)? {
        TypedValue::ZSet(z) => Ok(Some(z)),
        TypedValue::String(_) => Err(wrong_type_error()),
        _ => Err(wrong_type_error()),
    }
}

fn persist_zset(ctx: &AppContext, key: &[u8], zset: ZSet) {
    ctx.db.put_entry(
        key,
        ValueEntry {
            value: Bytes::from(encode_zset(&zset)),
            expire_at_ms: None,
        },
    );
}

fn persist_zset_or_delete(ctx: &AppContext, key: &[u8], zset: ZSet) {
    if zset.is_empty() {
        let _ = ctx.db.remove_entry(key);
    } else {
        persist_zset(ctx, key, zset);
    }
}

fn parse_score(raw: &[u8]) -> Result<f64, String> {
    let s = std::str::from_utf8(raw).map_err(|_| "ERR value is not a valid float".to_string())?;
    let value: f64 = s
        .parse()
        .map_err(|_| "ERR value is not a valid float".to_string())?;
    if !value.is_finite() {
        return Err("ERR value is not a valid float".to_string());
    }
    Ok(value)
}

/// Parse Redis score bound: "-inf", "+inf", "(value", "value"
fn parse_score_bound(raw: &[u8]) -> Result<(f64, bool), String> {
    if raw.eq_ignore_ascii_case(b"-inf") || raw == b"-inf" {
        return Ok((f64::NEG_INFINITY, false));
    }
    if raw.eq_ignore_ascii_case(b"+inf") || raw == b"+inf" {
        return Ok((f64::INFINITY, false));
    }
    let (value, excl) = if raw.first() == Some(&b'(') {
        (
            std::str::from_utf8(&raw[1..])
                .map_err(|_| "ERR min or max is not a float".to_string())?
                .parse::<f64>()
                .map_err(|_| "ERR min or max is not a float".to_string())?,
            true,
        )
    } else {
        (
            std::str::from_utf8(raw)
                .map_err(|_| "ERR min or max is not a float".to_string())?
                .parse::<f64>()
                .map_err(|_| "ERR min or max is not a float".to_string())?,
            false,
        )
    };
    if !value.is_finite() {
        return Err("ERR min or max is not a float".to_string());
    }
    Ok((value, excl))
}

/// Parse Redis lex bound: "-", "+", "[value", "(value"
fn parse_lex_bound(raw: &[u8]) -> Result<(Vec<u8>, bool), String> {
    if raw == b"-" {
        return Ok((Vec::new(), true));
    }
    if raw == b"+" {
        return Ok((Vec::new(), true));
    }
    if raw.is_empty() {
        return Err("ERR syntax error".to_string());
    }
    let (inc, val) = if raw[0] == b'[' {
        (true, raw[1..].to_vec())
    } else if raw[0] == b'(' {
        (false, raw[1..].to_vec())
    } else {
        return Err("ERR syntax error".to_string());
    };
    Ok((val, inc))
}

fn compute_zinter(ctx: &AppContext, keys: &[&[u8]]) -> Result<ZSet, String> {
    use std::collections::HashMap;
    if keys.is_empty() {
        return Ok(ZSet::new());
    }
    let first = required_arg(keys, 0)?;
    let Some(first_z) = get_zset(ctx, first)? else {
        return Ok(ZSet::new());
    };
    let mut member_scores: HashMap<Vec<u8>, f64> = first_z.iter().map(|(m, s)| (m, s)).collect();
    for key in &keys[1..] {
        let Some(other) = get_zset(ctx, key)? else {
            return Ok(ZSet::new());
        };
        let other_map: HashMap<_, _> = other.iter().map(|(m, s)| (m.clone(), s)).collect();
        let mut next_scores = HashMap::new();
        for (m, s) in &member_scores {
            if let Some(os) = other_map.get(m) {
                next_scores.insert(m.clone(), s + os);
            }
        }
        member_scores = next_scores;
        if member_scores.is_empty() {
            break;
        }
    }
    let mut result = ZSet::new();
    for (member, score) in member_scores {
        let _ = result.add(member, score);
    }
    Ok(result)
}

fn compute_zunion(ctx: &AppContext, keys: &[&[u8]]) -> Result<ZSet, String> {
    use std::collections::HashMap;
    let mut scores: HashMap<Vec<u8>, f64> = HashMap::new();
    for key in keys {
        if let Some(z) = get_zset(ctx, key)? {
            for (member, score) in z.iter() {
                *scores.entry(member).or_insert(0.0) += score;
            }
        }
    }
    let mut result = ZSet::new();
    for (member, score) in scores {
        let _ = result.add(member, score);
    }
    Ok(result)
}

fn parse_i64(raw: &[u8]) -> Result<i64, String> {
    std::str::from_utf8(raw)
        .map_err(|_| "ERR value is not an integer or out of range".to_string())?
        .parse::<i64>()
        .map_err(|_| "ERR value is not an integer or out of range".to_string())
}

fn parse_usize(raw: &[u8]) -> Result<usize, String> {
    std::str::from_utf8(raw)
        .map_err(|_| "ERR value is not an integer or out of range".to_string())?
        .parse::<usize>()
        .map_err(|_| "ERR value is not an integer or out of range".to_string())
}
