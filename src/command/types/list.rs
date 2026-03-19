use crate::app_context::AppContext;
use crate::command::shared::args::required_arg;
use crate::command::shared::typed_value::{
    TypedValue, decode_value, encode_list, wrong_type_error,
};
use crate::command::shared::wire::{append_array_header, append_bulk_items};
use crate::resp::{append_bulk_response, append_error_response, append_integer_response};
use crate::storage::ValueEntry;

macro_rules! list_commands {
    ($m:ident) => {
        $m!(list, blpop, cmd_blpop);
        $m!(list, brpop, cmd_brpop);
        $m!(list, brpoplpush, cmd_brpoplpush);
        $m!(list, lindex, cmd_lindex);
        $m!(list, linsert, cmd_linsert);
        $m!(list, llen, cmd_llen);
        $m!(list, lpop, cmd_lpop);
        $m!(list, lpos, cmd_lpos);
        $m!(list, lpush, cmd_lpush);
        $m!(list, lpushx, cmd_lpushx);
        $m!(list, lrange, cmd_lrange);
        $m!(list, lrem, cmd_lrem);
        $m!(list, lset, cmd_lset);
        $m!(list, ltrim, cmd_ltrim);
        $m!(list, rpop, cmd_rpop);
        $m!(list, rpoplpush, cmd_rpoplpush);
        $m!(list, rpush, cmd_rpush);
        $m!(list, rpushx, cmd_rpushx);
    };
}
pub(crate) use list_commands;

pub fn cmd_lpush(args: &[&[u8]], ctx: &AppContext, out: &mut Vec<u8>) -> Result<(), String> {
    push_impl(args, ctx, out, true, false)
}

pub fn cmd_lpushx(args: &[&[u8]], ctx: &AppContext, out: &mut Vec<u8>) -> Result<(), String> {
    push_impl(args, ctx, out, true, true)
}

pub fn cmd_rpush(args: &[&[u8]], ctx: &AppContext, out: &mut Vec<u8>) -> Result<(), String> {
    push_impl(args, ctx, out, false, false)
}

pub fn cmd_rpushx(args: &[&[u8]], ctx: &AppContext, out: &mut Vec<u8>) -> Result<(), String> {
    push_impl(args, ctx, out, false, true)
}

pub fn cmd_lpop(args: &[&[u8]], ctx: &AppContext, out: &mut Vec<u8>) -> Result<(), String> {
    pop_with_optional_count(args, ctx, out, true)
}

pub fn cmd_rpop(args: &[&[u8]], ctx: &AppContext, out: &mut Vec<u8>) -> Result<(), String> {
    pop_with_optional_count(args, ctx, out, false)
}

pub fn cmd_blpop(args: &[&[u8]], ctx: &AppContext, out: &mut Vec<u8>) -> Result<(), String> {
    blocking_pop(args, ctx, out, true)
}

pub fn cmd_brpop(args: &[&[u8]], ctx: &AppContext, out: &mut Vec<u8>) -> Result<(), String> {
    blocking_pop(args, ctx, out, false)
}

pub fn cmd_brpoplpush(args: &[&[u8]], ctx: &AppContext, out: &mut Vec<u8>) -> Result<(), String> {
    if args.len() != 3 {
        return Err("ERR wrong number of arguments for 'brpoplpush' command".to_string());
    }
    pop_push_between_lists(args[0], args[1], ctx, out, false)
}

pub fn cmd_rpoplpush(args: &[&[u8]], ctx: &AppContext, out: &mut Vec<u8>) -> Result<(), String> {
    if args.len() != 2 {
        return Err("ERR wrong number of arguments for 'rpoplpush' command".to_string());
    }
    pop_push_between_lists(args[0], args[1], ctx, out, false)
}

pub fn cmd_lindex(args: &[&[u8]], ctx: &AppContext, out: &mut Vec<u8>) -> Result<(), String> {
    let key = required_arg(args, 0)?;
    let index = parse_i64(required_arg(args, 1)?)?;
    let list = get_list(ctx, key)?.unwrap_or_default();
    let item = list_index(&list, index).and_then(|idx| list.get(idx).cloned());
    append_bulk_response(out, item.as_deref());
    Ok(())
}

pub fn cmd_linsert(args: &[&[u8]], ctx: &AppContext, out: &mut Vec<u8>) -> Result<(), String> {
    if args.len() != 4 {
        return Err("ERR wrong number of arguments for 'linsert' command".to_string());
    }
    let key = args[0];
    let mode = args[1];
    let pivot = args[2];
    let element = args[3];
    let Some(mut list) = get_list(ctx, key)? else {
        append_integer_response(out, 0);
        return Ok(());
    };
    let Some(pos) = list.iter().position(|item| item.as_slice() == pivot) else {
        append_integer_response(out, -1);
        return Ok(());
    };
    if mode.eq_ignore_ascii_case(b"before") {
        list.insert(pos, element.to_vec());
    } else if mode.eq_ignore_ascii_case(b"after") {
        list.insert(pos + 1, element.to_vec());
    } else {
        return Err("ERR syntax error".to_string());
    }
    let len = list.len() as i64;
    persist_list(ctx, key, list);
    append_integer_response(out, len);
    Ok(())
}

pub fn cmd_llen(args: &[&[u8]], ctx: &AppContext, out: &mut Vec<u8>) -> Result<(), String> {
    let key = required_arg(args, 0)?;
    let len = get_list(ctx, key)?.map(|v| v.len()).unwrap_or(0);
    append_integer_response(out, len as i64);
    Ok(())
}

pub fn cmd_lpos(args: &[&[u8]], ctx: &AppContext, out: &mut Vec<u8>) -> Result<(), String> {
    let key = required_arg(args, 0)?;
    let element = required_arg(args, 1)?;
    let list = get_list(ctx, key)?.unwrap_or_default();
    let found = list.iter().position(|item| item.as_slice() == element);
    match found {
        Some(idx) => append_integer_response(out, idx as i64),
        None => append_bulk_response(out, None),
    }
    Ok(())
}

pub fn cmd_lrange(args: &[&[u8]], ctx: &AppContext, out: &mut Vec<u8>) -> Result<(), String> {
    let key = required_arg(args, 0)?;
    let start = parse_i64(required_arg(args, 1)?)?;
    let end = parse_i64(required_arg(args, 2)?)?;
    let list = get_list(ctx, key)?.unwrap_or_default();
    let range = slice_indices(list.len(), start, end);
    let items = range.map(|(s, e)| list[s..=e].to_vec()).unwrap_or_default();
    append_bulk_items(out, &items);
    Ok(())
}

pub fn cmd_lrem(args: &[&[u8]], ctx: &AppContext, out: &mut Vec<u8>) -> Result<(), String> {
    let key = required_arg(args, 0)?;
    let count = parse_i64(required_arg(args, 1)?)?;
    let element = required_arg(args, 2)?;
    let Some(mut list) = get_list(ctx, key)? else {
        append_integer_response(out, 0);
        return Ok(());
    };
    let mut removed = 0_i64;
    if count == 0 {
        list.retain(|item| {
            let keep = item.as_slice() != element;
            if !keep {
                removed += 1;
            }
            keep
        });
    } else if count > 0 {
        let mut keep = Vec::with_capacity(list.len());
        for item in list {
            if removed < count && item.as_slice() == element {
                removed += 1;
            } else {
                keep.push(item);
            }
        }
        list = keep;
    } else {
        let mut keep = Vec::with_capacity(list.len());
        for item in list.into_iter().rev() {
            if removed < -count && item.as_slice() == element {
                removed += 1;
            } else {
                keep.push(item);
            }
        }
        keep.reverse();
        list = keep;
    }
    persist_list_or_delete(ctx, key, list);
    append_integer_response(out, removed);
    Ok(())
}

pub fn cmd_lset(args: &[&[u8]], ctx: &AppContext, out: &mut Vec<u8>) -> Result<(), String> {
    let key = required_arg(args, 0)?;
    let index = parse_i64(required_arg(args, 1)?)?;
    let element = required_arg(args, 2)?;
    let Some(mut list) = get_list(ctx, key)? else {
        return Err("ERR no such key".to_string());
    };
    let idx = list_index(&list, index).ok_or_else(|| "ERR index out of range".to_string())?;
    list[idx] = element.to_vec();
    persist_list(ctx, key, list);
    out.extend_from_slice(b"+OK\r\n");
    Ok(())
}

pub fn cmd_ltrim(args: &[&[u8]], ctx: &AppContext, out: &mut Vec<u8>) -> Result<(), String> {
    let key = required_arg(args, 0)?;
    let start = parse_i64(required_arg(args, 1)?)?;
    let end = parse_i64(required_arg(args, 2)?)?;
    let list = get_list(ctx, key)?.unwrap_or_default();
    if let Some((s, e)) = slice_indices(list.len(), start, end) {
        let trimmed = list[s..=e].to_vec();
        persist_list(ctx, key, trimmed);
    } else {
        let _ = ctx.db.remove_entry(key);
    }
    out.extend_from_slice(b"+OK\r\n");
    Ok(())
}

fn push_impl(
    args: &[&[u8]],
    ctx: &AppContext,
    out: &mut Vec<u8>,
    push_left: bool,
    must_exist: bool,
) -> Result<(), String> {
    let key = required_arg(args, 0)?;
    if args.len() < 2 {
        return Err("ERR wrong number of arguments for push command".to_string());
    }
    let existing = get_list(ctx, key)?;
    if must_exist && existing.is_none() {
        append_integer_response(out, 0);
        return Ok(());
    }
    let mut list = existing.unwrap_or_default();
    for value in &args[1..] {
        if push_left {
            list.insert(0, value.to_vec());
        } else {
            list.push(value.to_vec());
        }
    }
    let len = list.len() as i64;
    persist_list(ctx, key, list);
    append_integer_response(out, len);
    Ok(())
}

fn pop_with_optional_count(
    args: &[&[u8]],
    ctx: &AppContext,
    out: &mut Vec<u8>,
    left: bool,
) -> Result<(), String> {
    let key = required_arg(args, 0)?;
    let count = if args.len() > 1 {
        Some(parse_usize(args[1])?)
    } else {
        None
    };
    let Some(mut list) = get_list(ctx, key)? else {
        append_bulk_response(out, None);
        return Ok(());
    };
    match count {
        None => {
            let item = if left {
                if list.is_empty() {
                    None
                } else {
                    Some(list.remove(0))
                }
            } else {
                list.pop()
            };
            persist_list_or_delete(ctx, key, list);
            append_bulk_response(out, item.as_deref());
        }
        Some(n) => {
            let mut popped = Vec::new();
            for _ in 0..n {
                let item = if left {
                    if list.is_empty() {
                        None
                    } else {
                        Some(list.remove(0))
                    }
                } else {
                    list.pop()
                };
                match item {
                    Some(v) => popped.push(v),
                    None => break,
                }
            }
            persist_list_or_delete(ctx, key, list);
            append_bulk_items(out, &popped);
        }
    }
    Ok(())
}

fn blocking_pop(
    args: &[&[u8]],
    ctx: &AppContext,
    out: &mut Vec<u8>,
    left: bool,
) -> Result<(), String> {
    if args.len() < 2 {
        return Err("ERR wrong number of arguments for blocking pop command".to_string());
    }
    // timeout is currently accepted but not used for blocking behavior.
    for key in &args[..args.len() - 1] {
        let Some(mut list) = get_list(ctx, key)? else {
            continue;
        };
        let item = if left {
            if list.is_empty() {
                None
            } else {
                Some(list.remove(0))
            }
        } else {
            list.pop()
        };
        if let Some(value) = item {
            persist_list_or_delete(ctx, key, list);
            append_array_header(out, 2);
            append_bulk_response(out, Some(key));
            append_bulk_response(out, Some(&value));
            return Ok(());
        }
    }
    append_bulk_response(out, None);
    Ok(())
}

fn pop_push_between_lists(
    src: &[u8],
    dst: &[u8],
    ctx: &AppContext,
    out: &mut Vec<u8>,
    from_left: bool,
) -> Result<(), String> {
    let Some(mut src_list) = get_list(ctx, src)? else {
        append_bulk_response(out, None);
        return Ok(());
    };
    let item = if from_left {
        if src_list.is_empty() {
            None
        } else {
            Some(src_list.remove(0))
        }
    } else {
        src_list.pop()
    };
    let Some(value) = item else {
        append_bulk_response(out, None);
        return Ok(());
    };
    persist_list_or_delete(ctx, src, src_list);
    let mut dst_list = get_list(ctx, dst)?.unwrap_or_default();
    dst_list.insert(0, value.clone());
    persist_list(ctx, dst, dst_list);
    append_bulk_response(out, Some(&value));
    Ok(())
}

fn get_list(ctx: &AppContext, key: &[u8]) -> Result<Option<Vec<Vec<u8>>>, String> {
    let Some(entry) = ctx.db.get_entry(key) else {
        return Ok(None);
    };
    match decode_value(&entry.value)? {
        TypedValue::List(values) => Ok(Some(values)),
        TypedValue::String(_) => Err(wrong_type_error()),
        _ => Err(wrong_type_error()),
    }
}

fn persist_list(ctx: &AppContext, key: &[u8], list: Vec<Vec<u8>>) {
    ctx.db.put_entry(
        key,
        ValueEntry {
            value: encode_list(&list),
            expire_at_ms: None,
        },
    );
}

fn persist_list_or_delete(ctx: &AppContext, key: &[u8], list: Vec<Vec<u8>>) {
    if list.is_empty() {
        let _ = ctx.db.remove_entry(key);
    } else {
        persist_list(ctx, key, list);
    }
}

fn list_index(list: &[Vec<u8>], index: i64) -> Option<usize> {
    if list.is_empty() {
        return None;
    }
    let len = list.len() as i64;
    let idx = if index < 0 { len + index } else { index };
    if idx < 0 || idx >= len {
        None
    } else {
        Some(idx as usize)
    }
}

fn slice_indices(len: usize, start: i64, end: i64) -> Option<(usize, usize)> {
    if len == 0 {
        return None;
    }
    let list_len = len as i64;
    let mut s = if start < 0 { list_len + start } else { start };
    let mut e = if end < 0 { list_len + end } else { end };
    if s < 0 {
        s = 0;
    }
    if e >= list_len {
        e = list_len - 1;
    }
    if s > e || s >= list_len || e < 0 {
        return None;
    }
    Some((s as usize, e as usize))
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

#[allow(dead_code)]
fn _append_syntax_error(out: &mut Vec<u8>) {
    append_error_response(out, "ERR syntax error");
}

