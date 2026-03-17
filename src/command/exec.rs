use crate::app_context::AppContext;
use crate::command::{Command, SetCondition};
use crate::resp::{append_bulk_response, append_integer_response, append_simple_response};

pub fn execute_command(
    command: Command<'_>,
    ctx: &AppContext,
    out: &mut Vec<u8>,
) -> Result<(), String> {
    match command {
        Command::Ping(message) => {
            if let Some(message) = message {
                append_bulk_response(out, Some(message));
            } else {
                append_simple_response(out, "PONG");
            }
            Ok(())
        }
        Command::Set(key, value, ttl_ms, condition) => {
            let applied = match condition {
                SetCondition::None => {
                    if let Some(aof_engine) = ctx.aof.as_ref() {
                        aof_engine
                            .append_set(key, value, ttl_ms, condition)
                            .map_err(|e| format!("ERR AOF append failed: {e}"))?;
                    }
                    ctx.db.set_with_optional_ttl_ms(key, value, ttl_ms);
                    true
                }
                SetCondition::Nx => {
                    let applied = ctx.db.set_nx_with_optional_ttl_ms(key, value, ttl_ms);
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
                    let applied = ctx.db.set_xx_with_optional_ttl_ms(key, value, ttl_ms);
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
        Command::SetNx(key, value) => {
            let applied = ctx.db.set_nx_with_optional_ttl_ms(key, value, None);
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
        Command::SetEx(key, value, ttl_ms) | Command::PSetEx(key, value, ttl_ms) => {
            if let Some(aof_engine) = ctx.aof.as_ref() {
                aof_engine
                    .append_set(key, value, Some(ttl_ms), SetCondition::None)
                    .map_err(|e| format!("ERR AOF append failed: {e}"))?;
            }
            ctx.db.set_with_optional_ttl_ms(key, value, Some(ttl_ms));
            append_simple_response(out, "OK");
            Ok(())
        }
        Command::GetSet(key, value) => {
            let old = ctx.db.get_set(key, value);
            if let Some(aof_engine) = ctx.aof.as_ref() {
                aof_engine
                    .append_getset(key, value)
                    .map_err(|e| format!("ERR AOF append failed: {e}"))?;
            }
            append_bulk_response(out, old.as_deref());
            Ok(())
        }
        Command::MSet(pairs) => {
            if let Some(aof_engine) = ctx.aof.as_ref() {
                aof_engine
                    .append_mset(&pairs)
                    .map_err(|e| format!("ERR AOF append failed: {e}"))?;
            }
            for (key, value) in pairs {
                ctx.db.set(key, value);
            }
            append_simple_response(out, "OK");
            Ok(())
        }
        Command::MSetNx(pairs) => {
            let applied = ctx.db.mset_nx(&pairs);
            if applied {
                if let Some(aof_engine) = ctx.aof.as_ref() {
                    aof_engine
                        .append_msetnx(&pairs)
                        .map_err(|e| format!("ERR AOF append failed: {e}"))?;
                }
            }
            append_integer_response(out, if applied { 1 } else { 0 });
            Ok(())
        }
        Command::Get(key) => {
            let value = ctx.db.get(key);
            append_bulk_response(out, value.as_deref());
            Ok(())
        }
        Command::Ttl(key) => {
            let ttl = ctx.db.ttl_seconds(key);
            append_integer_response(out, ttl);
            Ok(())
        }
        Command::DbSize => {
            append_integer_response(out, ctx.db.dbsize() as i64);
            Ok(())
        }
        Command::Scan(cursor, count) => {
            let (next_cursor, keys) = ctx.db.scan(cursor, count);
            append_scan_response(out, next_cursor, &keys);
            Ok(())
        }
        Command::Del(keys) => {
            let mut deleted = 0_i64;
            for key in &keys {
                if ctx.db.delete(key) {
                    deleted += 1;
                }
            }
            if let Some(aof_engine) = ctx.aof.as_ref() {
                aof_engine
                    .append_del(&keys)
                    .map_err(|e| format!("ERR AOF append failed: {e}"))?;
            }
            append_integer_response(out, deleted);
            Ok(())
        }
        Command::Exists(keys) => {
            let mut exists = 0_i64;
            for key in keys {
                if ctx.db.exists(key) {
                    exists += 1;
                }
            }
            append_integer_response(out, exists);
            Ok(())
        }
    }
}

fn append_scan_response(out: &mut Vec<u8>, next_cursor: usize, keys: &[Vec<u8>]) {
    append_resp_array_header(out, 2);
    let next_cursor = next_cursor.to_string();
    append_bulk_response(out, Some(next_cursor.as_bytes()));
    append_resp_array_header(out, keys.len());
    for key in keys {
        append_bulk_response(out, Some(key));
    }
}

fn append_resp_array_header(out: &mut Vec<u8>, len: usize) {
    out.push(b'*');
    out.extend_from_slice(len.to_string().as_bytes());
    out.extend_from_slice(b"\r\n");
}
