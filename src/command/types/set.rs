use std::collections::BTreeSet;

use bytes::Bytes;

use crate::app_context::AppContext;
use crate::command::shared::args::required_arg;
use crate::command::shared::time::current_time_ms;
use crate::command::shared::typed_value::{TypedValue, decode_value, encode_set, wrong_type_error};
use crate::command::shared::wire::{append_array_header, append_bool_integer, append_bulk_items};
use crate::resp::{append_bulk_response, append_integer_response};
use crate::storage::ValueEntry;

macro_rules! set_commands {
    ($m:ident) => {
        $m!(set, sadd, cmd_sadd);
        $m!(set, scard, cmd_scard);
        $m!(set, sdiff, cmd_sdiff);
        $m!(set, sdiffstore, cmd_sdiffstore);
        $m!(set, sinter, cmd_sinter);
        $m!(set, sinterstore, cmd_sinterstore);
        $m!(set, sismember, cmd_sismember);
        $m!(set, smembers, cmd_smembers);
        $m!(set, smove, cmd_smove);
        $m!(set, spop, cmd_spop);
        $m!(set, srandmember, cmd_srandmember);
        $m!(set, srem, cmd_srem);
        $m!(set, sscan, cmd_sscan);
        $m!(set, sunion, cmd_sunion);
        $m!(set, sunionstore, cmd_sunionstore);
    };
}
pub(crate) use set_commands;

pub fn cmd_sadd(args: &[&[u8]], ctx: &AppContext, out: &mut Vec<u8>) -> Result<(), String> {
    let key = required_arg(args, 0)?;
    if args.len() < 2 {
        return Err("ERR wrong number of arguments for 'sadd' command".to_string());
    }
    let mut set = get_set(ctx, key)?.unwrap_or_default();
    let mut added = 0_i64;
    for member in &args[1..] {
        if set.insert((*member).to_vec()) {
            added += 1;
        }
    }
    persist_set(ctx, key, set);
    append_integer_response(out, added);
    Ok(())
}

pub fn cmd_scard(args: &[&[u8]], ctx: &AppContext, out: &mut Vec<u8>) -> Result<(), String> {
    let key = required_arg(args, 0)?;
    let size = get_set(ctx, key)?.map(|set| set.len()).unwrap_or(0);
    append_integer_response(out, size as i64);
    Ok(())
}

pub fn cmd_sdiff(args: &[&[u8]], ctx: &AppContext, out: &mut Vec<u8>) -> Result<(), String> {
    let result = compute_diff(ctx, args)?;
    let members = set_to_members(result);
    append_bulk_items(out, &members);
    Ok(())
}

pub fn cmd_sdiffstore(args: &[&[u8]], ctx: &AppContext, out: &mut Vec<u8>) -> Result<(), String> {
    let destination = required_arg(args, 0)?;
    if args.len() < 2 {
        return Err("ERR wrong number of arguments for 'sdiffstore' command".to_string());
    }
    let result = compute_diff(ctx, &args[1..])?;
    let len = result.len() as i64;
    persist_set_or_delete(ctx, destination, result);
    append_integer_response(out, len);
    Ok(())
}

pub fn cmd_sinter(args: &[&[u8]], ctx: &AppContext, out: &mut Vec<u8>) -> Result<(), String> {
    let result = compute_intersection(ctx, args)?;
    let members = set_to_members(result);
    append_bulk_items(out, &members);
    Ok(())
}

pub fn cmd_sinterstore(args: &[&[u8]], ctx: &AppContext, out: &mut Vec<u8>) -> Result<(), String> {
    let destination = required_arg(args, 0)?;
    if args.len() < 2 {
        return Err("ERR wrong number of arguments for 'sinterstore' command".to_string());
    }
    let result = compute_intersection(ctx, &args[1..])?;
    let len = result.len() as i64;
    persist_set_or_delete(ctx, destination, result);
    append_integer_response(out, len);
    Ok(())
}

pub fn cmd_sismember(args: &[&[u8]], ctx: &AppContext, out: &mut Vec<u8>) -> Result<(), String> {
    let key = required_arg(args, 0)?;
    let member = required_arg(args, 1)?;
    let exists = get_set(ctx, key)?
        .map(|set| set.contains(member))
        .unwrap_or(false);
    append_bool_integer(out, exists);
    Ok(())
}

pub fn cmd_smembers(args: &[&[u8]], ctx: &AppContext, out: &mut Vec<u8>) -> Result<(), String> {
    let key = required_arg(args, 0)?;
    let members = get_set(ctx, key)?.map(set_to_members).unwrap_or_default();
    append_bulk_items(out, &members);
    Ok(())
}

pub fn cmd_smove(args: &[&[u8]], ctx: &AppContext, out: &mut Vec<u8>) -> Result<(), String> {
    let source = required_arg(args, 0)?;
    let destination = required_arg(args, 1)?;
    let member = required_arg(args, 2)?;
    let Some(mut src_set) = get_set(ctx, source)? else {
        append_integer_response(out, 0);
        return Ok(());
    };
    if !src_set.remove(member) {
        append_integer_response(out, 0);
        return Ok(());
    }
    persist_set_or_delete(ctx, source, src_set);
    let mut dst_set = get_set(ctx, destination)?.unwrap_or_default();
    dst_set.insert(member.to_vec());
    persist_set(ctx, destination, dst_set);
    append_integer_response(out, 1);
    Ok(())
}

pub fn cmd_spop(args: &[&[u8]], ctx: &AppContext, out: &mut Vec<u8>) -> Result<(), String> {
    let key = required_arg(args, 0)?;
    let Some(mut set) = get_set(ctx, key)? else {
        if args.len() > 1 {
            append_array_header(out, 0);
        } else {
            append_bulk_response(out, None);
        }
        return Ok(());
    };
    if args.len() == 1 {
        let Some(member) = take_one_random_member(&mut set) else {
            append_bulk_response(out, None);
            return Ok(());
        };
        persist_set_or_delete(ctx, key, set);
        append_bulk_response(out, Some(&member));
        return Ok(());
    }
    let count = parse_i64(required_arg(args, 1)?)?;
    if count < 0 {
        return Err("ERR value is out of range, must be positive".to_string());
    }
    let count = count as usize;
    let mut popped = Vec::new();
    for _ in 0..count {
        let Some(member) = take_one_random_member(&mut set) else {
            break;
        };
        popped.push(member);
    }
    persist_set_or_delete(ctx, key, set);
    append_bulk_items(out, &popped);
    Ok(())
}

pub fn cmd_srandmember(args: &[&[u8]], ctx: &AppContext, out: &mut Vec<u8>) -> Result<(), String> {
    let key = required_arg(args, 0)?;
    let Some(set) = get_set(ctx, key)? else {
        if args.len() > 1 {
            append_array_header(out, 0);
        } else {
            append_bulk_response(out, None);
        }
        return Ok(());
    };
    let values: Vec<Vec<u8>> = set.into_iter().collect();
    if args.len() == 1 {
        let picked = pick_one_random_member(&values);
        append_bulk_response(out, picked.as_deref());
        return Ok(());
    }
    let count = parse_i64(required_arg(args, 1)?)?;
    if count >= 0 {
        let distinct = pick_distinct_members(&values, count as usize);
        append_bulk_items(out, &distinct);
    } else {
        let repeated = pick_repeated_members(&values, count.unsigned_abs() as usize);
        append_bulk_items(out, &repeated);
    }
    Ok(())
}

pub fn cmd_srem(args: &[&[u8]], ctx: &AppContext, out: &mut Vec<u8>) -> Result<(), String> {
    let key = required_arg(args, 0)?;
    if args.len() < 2 {
        return Err("ERR wrong number of arguments for 'srem' command".to_string());
    }
    let Some(mut set) = get_set(ctx, key)? else {
        append_integer_response(out, 0);
        return Ok(());
    };
    let mut removed = 0_i64;
    for member in &args[1..] {
        if set.remove(*member) {
            removed += 1;
        }
    }
    persist_set_or_delete(ctx, key, set);
    append_integer_response(out, removed);
    Ok(())
}

pub fn cmd_sscan(args: &[&[u8]], ctx: &AppContext, out: &mut Vec<u8>) -> Result<(), String> {
    let key = required_arg(args, 0)?;
    let cursor = parse_usize(required_arg(args, 1)?)?;
    if cursor != 0 {
        append_array_header(out, 2);
        append_bulk_response(out, Some(b"0"));
        append_array_header(out, 0);
        return Ok(());
    }
    let members = get_set(ctx, key)?.map(set_to_members).unwrap_or_default();
    append_array_header(out, 2);
    append_bulk_response(out, Some(b"0"));
    append_bulk_items(out, &members);
    Ok(())
}

pub fn cmd_sunion(args: &[&[u8]], ctx: &AppContext, out: &mut Vec<u8>) -> Result<(), String> {
    let union = compute_union(ctx, args)?;
    let members = set_to_members(union);
    append_bulk_items(out, &members);
    Ok(())
}

pub fn cmd_sunionstore(args: &[&[u8]], ctx: &AppContext, out: &mut Vec<u8>) -> Result<(), String> {
    let destination = required_arg(args, 0)?;
    if args.len() < 2 {
        return Err("ERR wrong number of arguments for 'sunionstore' command".to_string());
    }
    let union = compute_union(ctx, &args[1..])?;
    let len = union.len() as i64;
    persist_set_or_delete(ctx, destination, union);
    append_integer_response(out, len);
    Ok(())
}

fn get_set(ctx: &AppContext, key: &[u8]) -> Result<Option<BTreeSet<Vec<u8>>>, String> {
    let Some(entry) = ctx.db.get_entry(key) else {
        return Ok(None);
    };
    match decode_value(&entry.value)? {
        TypedValue::Set(values) => Ok(Some(values)),
        TypedValue::String(_) => Err(wrong_type_error()),
        _ => Err(wrong_type_error()),
    }
}

fn persist_set(ctx: &AppContext, key: &[u8], set: BTreeSet<Vec<u8>>) {
    ctx.db.put_entry(
        key,
        ValueEntry {
            value: Bytes::from(encode_set(&set)),
            expire_at_ms: None,
        },
    );
}

fn persist_set_or_delete(ctx: &AppContext, key: &[u8], set: BTreeSet<Vec<u8>>) {
    if set.is_empty() {
        let _ = ctx.db.remove_entry(key);
    } else {
        persist_set(ctx, key, set);
    }
}

fn compute_diff(ctx: &AppContext, keys: &[&[u8]]) -> Result<BTreeSet<Vec<u8>>, String> {
    let first_key = required_arg(keys, 0)?;
    let Some(mut base) = get_set(ctx, first_key)? else {
        return Ok(BTreeSet::new());
    };
    for key in &keys[1..] {
        if let Some(other) = get_set(ctx, key)? {
            for member in other {
                base.remove(&member);
            }
        }
    }
    Ok(base)
}

fn compute_intersection(ctx: &AppContext, keys: &[&[u8]]) -> Result<BTreeSet<Vec<u8>>, String> {
    let first_key = required_arg(keys, 0)?;
    let Some(mut result) = get_set(ctx, first_key)? else {
        return Ok(BTreeSet::new());
    };
    for key in &keys[1..] {
        let Some(other) = get_set(ctx, key)? else {
            return Ok(BTreeSet::new());
        };
        result.retain(|member| other.contains(member));
        if result.is_empty() {
            break;
        }
    }
    Ok(result)
}

fn compute_union(ctx: &AppContext, keys: &[&[u8]]) -> Result<BTreeSet<Vec<u8>>, String> {
    if keys.is_empty() {
        return Err("ERR wrong number of arguments for 'sunion' command".to_string());
    }
    let mut out = BTreeSet::new();
    for key in keys {
        if let Some(set) = get_set(ctx, key)? {
            out.extend(set);
        }
    }
    Ok(out)
}

fn set_to_members(set: BTreeSet<Vec<u8>>) -> Vec<Vec<u8>> {
    set.into_iter().collect()
}

fn parse_i64(raw: &[u8]) -> Result<i64, String> {
    std::str::from_utf8(raw)
        .map_err(|_| "ERR value is not an integer or out of range".to_string())?
        .parse::<i64>()
        .map_err(|_| "ERR value is not an integer or out of range".to_string())
}

fn parse_usize(raw: &[u8]) -> Result<usize, String> {
    std::str::from_utf8(raw)
        .map_err(|_| "ERR invalid cursor".to_string())?
        .parse::<usize>()
        .map_err(|_| "ERR invalid cursor".to_string())
}

fn take_one_random_member(set: &mut BTreeSet<Vec<u8>>) -> Option<Vec<u8>> {
    if set.is_empty() {
        return None;
    }
    let values: Vec<Vec<u8>> = set.iter().cloned().collect();
    let picked = pick_one_random_member(&values)?;
    set.remove(&picked);
    Some(picked)
}

fn pick_one_random_member(values: &[Vec<u8>]) -> Option<Vec<u8>> {
    if values.is_empty() {
        return None;
    }
    let seed = seed_from_time();
    let idx = (seed as usize) % values.len();
    Some(values[idx].clone())
}

fn pick_distinct_members(values: &[Vec<u8>], count: usize) -> Vec<Vec<u8>> {
    if values.is_empty() || count == 0 {
        return Vec::new();
    }
    let target = count.min(values.len());
    let start = (seed_from_time() as usize) % values.len();
    let mut out = Vec::with_capacity(target);
    for i in 0..target {
        let idx = (start + i) % values.len();
        out.push(values[idx].clone());
    }
    out
}

fn pick_repeated_members(values: &[Vec<u8>], count: usize) -> Vec<Vec<u8>> {
    if values.is_empty() || count == 0 {
        return Vec::new();
    }
    let mut seed = seed_from_time();
    let mut out = Vec::with_capacity(count);
    for _ in 0..count {
        seed = xorshift64(seed);
        let idx = (seed as usize) % values.len();
        out.push(values[idx].clone());
    }
    out
}

fn seed_from_time() -> u64 {
    let now = current_time_ms();
    xorshift64(now ^ 0x9E37_79B9_7F4A_7C15)
}

fn xorshift64(mut state: u64) -> u64 {
    if state == 0 {
        state = 0xA5A5_5A5A_DEAD_BEEF;
    }
    state ^= state << 13;
    state ^= state >> 7;
    state ^= state << 17;
    state
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::{
        parse_i64, parse_usize, pick_distinct_members, pick_one_random_member,
        pick_repeated_members, set_to_members, take_one_random_member, xorshift64,
    };

    #[test]
    fn parse_helpers_validate_integer_and_cursor_inputs() {
        assert_eq!(parse_i64(b"-2").expect("valid"), -2);
        assert_eq!(
            parse_i64(b"abc").expect_err("invalid"),
            "ERR value is not an integer or out of range"
        );

        assert_eq!(parse_usize(b"12").expect("valid"), 12);
        assert_eq!(
            parse_usize(b"-1").expect_err("invalid"),
            "ERR invalid cursor"
        );
    }

    #[test]
    fn set_to_members_returns_sorted_members_from_btreeset() {
        let mut set = BTreeSet::new();
        set.insert(b"b".to_vec());
        set.insert(b"a".to_vec());
        let members = set_to_members(set);
        assert_eq!(members, vec![b"a".to_vec(), b"b".to_vec()]);
    }

    #[test]
    fn random_member_helpers_return_only_existing_members() {
        let values = vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()];
        let picked = pick_one_random_member(&values).expect("non-empty");
        assert!(values.contains(&picked));

        let distinct = pick_distinct_members(&values, 2);
        assert_eq!(distinct.len(), 2);
        assert!(distinct.iter().all(|m| values.contains(m)));

        let repeated = pick_repeated_members(&values, 5);
        assert_eq!(repeated.len(), 5);
        assert!(repeated.iter().all(|m| values.contains(m)));
    }

    #[test]
    fn take_one_random_member_removes_selected_value() {
        let mut set = BTreeSet::new();
        set.insert(b"a".to_vec());
        set.insert(b"b".to_vec());
        let before = set.len();

        let removed = take_one_random_member(&mut set).expect("should remove one");
        assert_eq!(set.len(), before - 1);
        assert!(!set.contains(&removed));
    }

    #[test]
    fn xorshift64_uses_fallback_for_zero_seed() {
        let out = xorshift64(0);
        assert_ne!(out, 0);
    }
}
