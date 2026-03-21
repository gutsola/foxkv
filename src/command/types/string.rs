use bytes::Bytes;

use crate::app_context::AppContext;
use crate::command::shared::args::required_arg;
use crate::command::shared::time::current_time_ms;
use crate::command::SetCondition;
use crate::resp::{append_bulk_response, append_integer_response, append_simple_response, parse_ascii_u64};
use crate::storage::ValueEntry;

macro_rules! string_commands {
    ($m:ident) => {
        // Hot-path first for better branch prediction.
        $m!(string, get, cmd_get);
        $m!(string, set, cmd_set);
        $m!(string, mget, cmd_mget);
        $m!(string, mset, cmd_mset);
        $m!(string, incr, cmd_incr);
        $m!(string, decr, cmd_decr);
        $m!(string, incrby, cmd_incrby);
        $m!(string, decrby, cmd_decrby);
        $m!(string, setnx, cmd_setnx);
        $m!(string, setex, cmd_setex);
        $m!(string, psetex, cmd_psetex);
        $m!(string, append, cmd_append);
        $m!(string, strlen, cmd_strlen);
        $m!(string, getrange, cmd_getrange);
        $m!(string, setrange, cmd_setrange);
        $m!(string, getset, cmd_getset);
        $m!(string, msetnx, cmd_msetnx);
        $m!(string, incrbyfloat, cmd_incrbyfloat);
        $m!(string, substr, cmd_substr);
    };
}
pub(crate) use string_commands;

pub fn cmd_append(args: &[&[u8]], ctx: &AppContext, out: &mut Vec<u8>) -> Result<(), String> {
    let key = required_arg(args, 0)?;
    let suffix = required_arg(args, 1)?;
    let mut entry = ctx.db.get_entry(key).unwrap_or(ValueEntry {
        value: Bytes::new(),
        expire_at_ms: None,
    });
    let mut value = entry.value.to_vec();
    value.extend_from_slice(suffix);
    entry.value = Bytes::from(value);
    if let Some(aof_engine) = ctx.aof.as_ref() {
        aof_engine
            .append_append(key, suffix)
            .map_err(|e| format!("ERR AOF append failed: {e}"))?;
    }
    let new_len = entry.value.len() as i64;
    ctx.db.put_entry(key, entry);
    append_integer_response(out, new_len);
    Ok(())
}

pub fn cmd_decr(args: &[&[u8]], ctx: &AppContext, out: &mut Vec<u8>) -> Result<(), String> {
    let key = required_arg(args, 0)?;
    execute_integer_delta(ctx, out, key, -1, b"DECR")
}

pub fn cmd_decrby(args: &[&[u8]], ctx: &AppContext, out: &mut Vec<u8>) -> Result<(), String> {
    let key = required_arg(args, 0)?;
    let delta_raw = required_arg(args, 1)?;
    let delta = parse_i64_argument(delta_raw)?;
    let negative_delta = delta
        .checked_neg()
        .ok_or_else(|| "ERR value is not an integer or out of range".to_string())?;
    execute_integer_delta_with_raw(ctx, out, key, negative_delta, b"DECRBY", Some(delta_raw))
}

pub fn cmd_get(args: &[&[u8]], ctx: &AppContext, out: &mut Vec<u8>) -> Result<(), String> {
    let key = required_arg(args, 0)?;
    let value = ctx.db.get_entry(key).map(|entry| entry.value);
    append_bulk_response(out, value.as_ref().map(|v| v.as_ref()));
    Ok(())
}

pub fn cmd_getrange(args: &[&[u8]], ctx: &AppContext, out: &mut Vec<u8>) -> Result<(), String> {
    let key = required_arg(args, 0)?;
    let start = parse_i64_argument(required_arg(args, 1)?)?;
    let end = parse_i64_argument(required_arg(args, 2)?)?;
    append_slice_range(ctx, out, key, start, end);
    Ok(())
}

pub fn cmd_getset(args: &[&[u8]], ctx: &AppContext, out: &mut Vec<u8>) -> Result<(), String> {
    let key = required_arg(args, 0)?;
    let value = required_arg(args, 1)?;
    let old = ctx.db.get_entry(key).map(|entry| entry.value);
    ctx.db.put_entry(
        key,
        ValueEntry {
            value: Bytes::copy_from_slice(value),
            expire_at_ms: None,
        },
    );
    if let Some(aof_engine) = ctx.aof.as_ref() {
        aof_engine
            .append_getset(key, value)
            .map_err(|e| format!("ERR AOF append failed: {e}"))?;
    }
    append_bulk_response(out, old.as_deref());
    Ok(())
}

pub fn cmd_incr(args: &[&[u8]], ctx: &AppContext, out: &mut Vec<u8>) -> Result<(), String> {
    let key = required_arg(args, 0)?;
    execute_integer_delta(ctx, out, key, 1, b"INCR")
}

pub fn cmd_incrby(args: &[&[u8]], ctx: &AppContext, out: &mut Vec<u8>) -> Result<(), String> {
    let key = required_arg(args, 0)?;
    let delta_raw = required_arg(args, 1)?;
    let delta = parse_i64_argument(delta_raw)?;
    execute_integer_delta_with_raw(ctx, out, key, delta, b"INCRBY", Some(delta_raw))
}

pub fn cmd_incrbyfloat(args: &[&[u8]], ctx: &AppContext, out: &mut Vec<u8>) -> Result<(), String> {
    let key = required_arg(args, 0)?;
    let increment_raw = required_arg(args, 1)?;
    let increment = parse_f64_argument(increment_raw)?;
    let entry = ctx.db.get_entry(key);
    let current = match entry.as_ref() {
        Some(v) => parse_f64_from_value(&v.value)?,
        None => 0.0,
    };
    let next = current + increment;
    if !next.is_finite() {
        return Err("ERR increment would produce NaN or Infinity".to_string());
    }
    if let Some(aof_engine) = ctx.aof.as_ref() {
        aof_engine
            .append_incrbyfloat(key, increment_raw)
            .map_err(|e| format!("ERR AOF append failed: {e}"))?;
    }
    let expire_at_ms = entry.and_then(|v| v.expire_at_ms);
    let next_bytes = next.to_string().into_bytes();
    ctx.db.put_entry(
        key,
        ValueEntry {
            value: Bytes::from(next_bytes.clone()),
            expire_at_ms,
        },
    );
    append_bulk_response(out, Some(&next_bytes));
    Ok(())
}

pub fn cmd_mget(args: &[&[u8]], ctx: &AppContext, out: &mut Vec<u8>) -> Result<(), String> {
    append_array_header(out, args.len());
    for key in args {
        let value = ctx.db.get_entry(key).map(|entry| entry.value);
        append_bulk_response(out, value.as_ref().map(|v| v.as_ref()));
    }
    Ok(())
}

pub fn cmd_mset(args: &[&[u8]], ctx: &AppContext, out: &mut Vec<u8>) -> Result<(), String> {
    if args.len() % 2 != 0 {
        return Err("ERR syntax error".to_string());
    }
    if let Some(aof_engine) = ctx.aof.as_ref() {
        aof_engine
            .append_mset_args(args)
            .map_err(|e| format!("ERR AOF append failed: {e}"))?;
    }
    let mut i = 0_usize;
    while i < args.len() {
        let key = args[i];
        let value = args[i + 1];
        ctx.db.put_entry(
            key,
            ValueEntry {
                value: Bytes::copy_from_slice(value),
                expire_at_ms: None,
            },
        );
        i += 2;
    }
    append_simple_response(out, "OK");
    Ok(())
}

pub fn cmd_msetnx(args: &[&[u8]], ctx: &AppContext, out: &mut Vec<u8>) -> Result<(), String> {
    if args.len() % 2 != 0 {
        return Err("ERR syntax error".to_string());
    }
    let mut exists = false;
    let mut i = 0_usize;
    while i < args.len() {
        if ctx.db.contains_live_key(args[i]) {
            exists = true;
            break;
        }
        i += 2;
    }
    let applied = if exists {
        false
    } else {
        let mut i = 0_usize;
        while i < args.len() {
            let key = args[i];
            let value = args[i + 1];
            ctx.db.put_entry(
                key,
                ValueEntry {
                    value: Bytes::copy_from_slice(value),
                    expire_at_ms: None,
                },
            );
            i += 2;
        }
        true
    };
    if applied {
        if let Some(aof_engine) = ctx.aof.as_ref() {
            aof_engine
                .append_msetnx_args(args)
                .map_err(|e| format!("ERR AOF append failed: {e}"))?;
        }
    }
    append_integer_response(out, if applied { 1 } else { 0 });
    Ok(())
}

pub fn cmd_psetex(args: &[&[u8]], ctx: &AppContext, out: &mut Vec<u8>) -> Result<(), String> {
    let key = required_arg(args, 0)?;
    let ttl_raw = required_arg(args, 1)?;
    let value = required_arg(args, 2)?;
    let ttl_ms = parse_ttl_ms(b"px", ttl_raw)?;
    set_with_ttl(ctx, out, key, value, ttl_ms)
}

pub fn cmd_set(args: &[&[u8]], ctx: &AppContext, out: &mut Vec<u8>) -> Result<(), String> {
    let key = required_arg(args, 0)?;
    let value = required_arg(args, 1)?;
    if args.len() == 2 {
        if let Some(aof_engine) = ctx.aof.as_ref() {
            aof_engine
                .append_set(key, value, None, SetCondition::None)
                .map_err(|e| format!("ERR AOF append failed: {e}"))?;
        }
        ctx.db.put_entry(
            key,
            ValueEntry {
                value: Bytes::copy_from_slice(value),
                expire_at_ms: None,
            },
        );
        append_simple_response(out, "OK");
        return Ok(());
    }
    let (ttl_ms, condition) = parse_set_options(&args[2..])?;
    let new_entry = build_entry(value, ttl_ms);
    let applied = match condition {
        SetCondition::None => {
            if let Some(aof_engine) = ctx.aof.as_ref() {
                aof_engine
                    .append_set(key, value, ttl_ms, condition)
                    .map_err(|e| format!("ERR AOF append failed: {e}"))?;
            }
            ctx.db.put_entry(key, new_entry);
            true
        }
        SetCondition::Nx => {
            let applied = ctx.db.put_if_absent(key, new_entry);
            if applied {
                if let Some(aof_engine) = ctx.aof.as_ref() {
                    aof_engine
                        .append_set(key, value, ttl_ms, condition)
                        .map_err(|e| format!("ERR AOF append failed: {e}"))?;
                }
            }
            applied
        }
        SetCondition::Xx => {
            let applied = ctx.db.put_if_present(key, new_entry);
            if applied {
                if let Some(aof_engine) = ctx.aof.as_ref() {
                    aof_engine
                        .append_set(key, value, ttl_ms, condition)
                        .map_err(|e| format!("ERR AOF append failed: {e}"))?;
                }
            }
            applied
        }
    };
    if applied {
        append_simple_response(out, "OK");
    } else {
        append_bulk_response(out, None);
    }
    Ok(())
}

pub fn cmd_setex(args: &[&[u8]], ctx: &AppContext, out: &mut Vec<u8>) -> Result<(), String> {
    let key = required_arg(args, 0)?;
    let ttl_raw = required_arg(args, 1)?;
    let value = required_arg(args, 2)?;
    let ttl_ms = parse_ttl_ms(b"ex", ttl_raw)?;
    set_with_ttl(ctx, out, key, value, ttl_ms)
}

pub fn cmd_setnx(args: &[&[u8]], ctx: &AppContext, out: &mut Vec<u8>) -> Result<(), String> {
    let key = required_arg(args, 0)?;
    let value = required_arg(args, 1)?;
    let applied = ctx.db.put_if_absent(
        key,
        ValueEntry {
            value: Bytes::copy_from_slice(value),
            expire_at_ms: None,
        },
    );
    if applied {
        if let Some(aof_engine) = ctx.aof.as_ref() {
            aof_engine
                .append_setnx(key, value)
                .map_err(|e| format!("ERR AOF append failed: {e}"))?;
        }
    }
    append_integer_response(out, if applied { 1 } else { 0 });
    Ok(())
}

pub fn cmd_setrange(args: &[&[u8]], ctx: &AppContext, out: &mut Vec<u8>) -> Result<(), String> {
    let key = required_arg(args, 0)?;
    let offset_raw = required_arg(args, 1)?;
    let value = required_arg(args, 2)?;
    let offset = parse_offset(offset_raw)?;
    let entry = ctx.db.get_entry(key);
    if value.is_empty() {
        let current_len = entry.map(|v| v.value.len() as i64).unwrap_or(0);
        append_integer_response(out, current_len);
        return Ok(());
    }
    let mut current = entry.unwrap_or(ValueEntry {
        value: Bytes::new(),
        expire_at_ms: None,
    });
    let mut current_value = current.value.to_vec();
    let new_len = offset
        .checked_add(value.len())
        .ok_or_else(|| "ERR offset is out of range".to_string())?;
    if current_value.len() < offset {
        current_value.resize(offset, 0);
    }
    if current_value.len() < new_len {
        current_value.resize(new_len, 0);
    }
    current_value[offset..offset + value.len()].copy_from_slice(value);
    if let Some(aof_engine) = ctx.aof.as_ref() {
        aof_engine
            .append_setrange(key, offset_raw, value)
            .map_err(|e| format!("ERR AOF append failed: {e}"))?;
    }
    let len = current_value.len() as i64;
    current.value = Bytes::from(current_value);
    ctx.db.put_entry(key, current);
    append_integer_response(out, len);
    Ok(())
}

pub fn cmd_strlen(args: &[&[u8]], ctx: &AppContext, out: &mut Vec<u8>) -> Result<(), String> {
    let key = required_arg(args, 0)?;
    let len = ctx.db.get_entry(key).map(|entry| entry.value.len()).unwrap_or(0);
    append_integer_response(out, len as i64);
    Ok(())
}

pub fn cmd_substr(args: &[&[u8]], ctx: &AppContext, out: &mut Vec<u8>) -> Result<(), String> {
    let key = required_arg(args, 0)?;
    let start = parse_i64_argument(required_arg(args, 1)?)?;
    let end = parse_i64_argument(required_arg(args, 2)?)?;
    append_slice_range(ctx, out, key, start, end);
    Ok(())
}

fn build_entry(value: &[u8], ttl_ms: Option<u64>) -> ValueEntry {
    ValueEntry {
        value: Bytes::copy_from_slice(value),
        expire_at_ms: ttl_ms.map(|ttl| current_time_ms().saturating_add(ttl)),
    }
}

fn execute_integer_delta(
    ctx: &AppContext,
    out: &mut Vec<u8>,
    key: &[u8],
    delta: i64,
    command: &[u8],
) -> Result<(), String> {
    execute_integer_delta_with_raw(ctx, out, key, delta, command, None)
}

fn execute_integer_delta_with_raw(
    ctx: &AppContext,
    out: &mut Vec<u8>,
    key: &[u8],
    delta: i64,
    command: &[u8],
    raw_delta: Option<&[u8]>,
) -> Result<(), String> {
    let entry = ctx.db.get_entry(key);
    let current = match entry.as_ref() {
        Some(v) => parse_i64_from_value(&v.value)?,
        None => 0,
    };
    let next = current
        .checked_add(delta)
        .ok_or_else(|| "ERR value is not an integer or out of range".to_string())?;
    if let Some(aof_engine) = ctx.aof.as_ref() {
        let append_result = match raw_delta {
            Some(arg) if command.eq_ignore_ascii_case(b"INCRBY") => aof_engine.append_incrby(key, arg),
            Some(arg) if command.eq_ignore_ascii_case(b"DECRBY") => aof_engine.append_decrby(key, arg),
            _ if command.eq_ignore_ascii_case(b"INCR") => aof_engine.append_incr(key),
            _ if command.eq_ignore_ascii_case(b"DECR") => aof_engine.append_decr(key),
            _ => Ok(()),
        };
        append_result.map_err(|e| format!("ERR AOF append failed: {e}"))?;
    }
    let expire_at_ms = entry.and_then(|v| v.expire_at_ms);
    let next_bytes = next.to_string().into_bytes();
    ctx.db.put_entry(
        key,
        ValueEntry {
            value: Bytes::from(next_bytes),
            expire_at_ms,
        },
    );
    append_integer_response(out, next);
    Ok(())
}

fn parse_i64_argument(raw: &[u8]) -> Result<i64, String> {
    std::str::from_utf8(raw)
        .map_err(|_| "ERR value is not an integer or out of range".to_string())?
        .parse::<i64>()
        .map_err(|_| "ERR value is not an integer or out of range".to_string())
}

fn parse_i64_from_value(raw: &[u8]) -> Result<i64, String> {
    parse_i64_argument(raw)
}

fn parse_f64_argument(raw: &[u8]) -> Result<f64, String> {
    let value = std::str::from_utf8(raw)
        .map_err(|_| "ERR value is not a valid float".to_string())?
        .parse::<f64>()
        .map_err(|_| "ERR value is not a valid float".to_string())?;
    if !value.is_finite() {
        return Err("ERR value is not a valid float".to_string());
    }
    Ok(value)
}

fn parse_f64_from_value(raw: &[u8]) -> Result<f64, String> {
    parse_f64_argument(raw)
}

fn parse_offset(raw: &[u8]) -> Result<usize, String> {
    let offset = parse_ascii_u64(raw).map_err(|_| "ERR offset is out of range".to_string())?;
    usize::try_from(offset).map_err(|_| "ERR offset is out of range".to_string())
}

fn slice_by_redis_range(value: &[u8], start: i64, end: i64) -> &[u8] {
    if value.is_empty() {
        return &[];
    }
    let len = value.len() as i64;
    let mut s = normalize_index(start, len);
    let mut e = normalize_index(end, len);
    if s < 0 {
        s = 0;
    }
    if e < 0 {
        return &[];
    }
    if s >= len {
        return &[];
    }
    if e >= len {
        e = len - 1;
    }
    if s > e {
        return &[];
    }
    &value[s as usize..=e as usize]
}

fn normalize_index(index: i64, len: i64) -> i64 {
    if index < 0 {
        len + index
    } else {
        index
    }
}

fn append_array_header(out: &mut Vec<u8>, len: usize) {
    out.push(b'*');
    out.extend_from_slice(len.to_string().as_bytes());
    out.extend_from_slice(b"\r\n");
}

fn set_with_ttl(
    ctx: &AppContext,
    out: &mut Vec<u8>,
    key: &[u8],
    value: &[u8],
    ttl_ms: u64,
) -> Result<(), String> {
    if let Some(aof_engine) = ctx.aof.as_ref() {
        aof_engine
            .append_set(key, value, Some(ttl_ms), SetCondition::None)
            .map_err(|e| format!("ERR AOF append failed: {e}"))?;
    }
    ctx.db.put_entry(
        key,
        ValueEntry {
            value: Bytes::copy_from_slice(value),
            expire_at_ms: Some(current_time_ms().saturating_add(ttl_ms)),
        },
    );
    append_simple_response(out, "OK");
    Ok(())
}

fn parse_set_options(options: &[&[u8]]) -> Result<(Option<u64>, SetCondition), String> {
    let mut ttl_ms = None;
    let mut condition = SetCondition::None;
    let mut i = 0_usize;
    while i < options.len() {
        let token = options[i];
        if token.len() == 2 {
            let b0 = token[0] | 0x20;
            let b1 = token[1] | 0x20;
            if b0 == b'n' && b1 == b'x' {
                if !matches!(condition, SetCondition::None) {
                    return Err("ERR syntax error".to_string());
                }
                condition = SetCondition::Nx;
                i += 1;
                continue;
            }
            if b0 == b'x' && b1 == b'x' {
                if !matches!(condition, SetCondition::None) {
                    return Err("ERR syntax error".to_string());
                }
                condition = SetCondition::Xx;
                i += 1;
                continue;
            }
            if (b0 == b'e' && b1 == b'x') || (b0 == b'p' && b1 == b'x') {
                if i + 1 >= options.len() || ttl_ms.is_some() {
                    return Err("ERR syntax error".to_string());
                }
                ttl_ms = Some(parse_ttl_ms(token, options[i + 1])?);
                i += 2;
                continue;
            }
        }
        return Err("ERR syntax error".to_string());
    }
    Ok((ttl_ms, condition))
}

fn parse_ttl_ms(ttl_kind: &[u8], ttl_raw: &[u8]) -> Result<u64, String> {
    let ttl_value = parse_ascii_u64(ttl_raw)
        .map_err(|_| "ERR invalid expire time in 'set' command".to_string())?;
    if ttl_value == 0 {
        return Err("ERR invalid expire time in 'set' command".to_string());
    }
    if ttl_kind.eq_ignore_ascii_case(b"px") {
        return Ok(ttl_value);
    }
    if ttl_kind.eq_ignore_ascii_case(b"ex") {
        return ttl_value
            .checked_mul(1000)
            .ok_or_else(|| "ERR invalid expire time in 'set' command".to_string());
    }
    Err("ERR syntax error".to_string())
}

fn append_slice_range(ctx: &AppContext, out: &mut Vec<u8>, key: &[u8], start: i64, end: i64) {
    let value = ctx.db.get_entry(key).map(|entry| entry.value).unwrap_or_default();
    let slice = slice_by_redis_range(&value, start, end);
    append_bulk_response(out, Some(slice));
}

#[cfg(test)]
mod tests {
    use super::{
        normalize_index, parse_f64_argument, parse_i64_argument, parse_offset, parse_set_options,
        parse_ttl_ms, slice_by_redis_range, SetCondition,
    };

    #[test]
    fn parse_number_helpers_validate_input_and_ranges() {
        assert_eq!(parse_i64_argument(b"-12").expect("valid"), -12);
        assert_eq!(
            parse_i64_argument(b"abc").expect_err("invalid"),
            "ERR value is not an integer or out of range"
        );

        assert_eq!(parse_f64_argument(b"1.25").expect("valid"), 1.25);
        assert_eq!(
            parse_f64_argument(b"NaN").expect_err("nan invalid"),
            "ERR value is not a valid float"
        );

        assert_eq!(parse_offset(b"5").expect("valid"), 5);
        assert_eq!(
            parse_offset(b"-1").expect_err("negative invalid"),
            "ERR offset is out of range"
        );
    }

    #[test]
    fn slice_by_redis_range_handles_negative_and_out_of_bounds_indices() {
        let v = b"abcdef";
        assert_eq!(slice_by_redis_range(v, 0, 2), b"abc");
        assert_eq!(slice_by_redis_range(v, -3, -1), b"def");
        assert_eq!(slice_by_redis_range(v, 10, 20), b"");
        assert_eq!(slice_by_redis_range(v, 3, 1), b"");
    }

    #[test]
    fn normalize_index_converts_negative_index_relative_to_length() {
        assert_eq!(normalize_index(-1, 6), 5);
        assert_eq!(normalize_index(-6, 6), 0);
        assert_eq!(normalize_index(2, 6), 2);
    }

    #[test]
    fn parse_set_options_supports_nx_xx_and_ttl_modifiers() {
        let (ttl, cond) = parse_set_options(&[b"nx"]).expect("valid");
        assert_eq!(ttl, None);
        assert!(matches!(cond, SetCondition::Nx));

        let (ttl2, cond2) = parse_set_options(&[b"EX", b"2", b"XX"]).expect("valid");
        assert_eq!(ttl2, Some(2000));
        assert!(matches!(cond2, SetCondition::Xx));
    }

    #[test]
    fn parse_set_options_rejects_conflicting_or_invalid_tokens() {
        match parse_set_options(&[b"NX", b"XX"]) {
            Ok(_) => panic!("expected syntax error"),
            Err(err) => assert_eq!(err, "ERR syntax error"),
        }
        match parse_set_options(&[b"PX"]) {
            Ok(_) => panic!("expected syntax error"),
            Err(err) => assert_eq!(err, "ERR syntax error"),
        }
        match parse_set_options(&[b"EX", b"0"]) {
            Ok(_) => panic!("expected invalid ttl"),
            Err(err) => assert_eq!(err, "ERR invalid expire time in 'set' command"),
        }
    }

    #[test]
    fn parse_ttl_ms_validates_kind_and_zero_value() {
        assert_eq!(parse_ttl_ms(b"px", b"150").expect("px"), 150);
        assert_eq!(parse_ttl_ms(b"ex", b"2").expect("ex"), 2000);
        assert_eq!(
            parse_ttl_ms(b"ex", b"0").expect_err("invalid"),
            "ERR invalid expire time in 'set' command"
        );
        assert_eq!(
            parse_ttl_ms(b"unknown", b"1").expect_err("invalid"),
            "ERR syntax error"
        );
    }
}
