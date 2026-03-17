use crate::app_context::AppContext;
use crate::command::Command;
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
        Command::Set(key, value, ttl_ms) => {
            if let Some(aof_engine) = ctx.aof.as_ref() {
                aof_engine
                    .append_set(key, value, ttl_ms)
                    .map_err(|e| format!("ERR AOF append failed: {e}"))?;
            }
            if let Some(ttl_ms) = ttl_ms {
                ctx.db.set_with_ttl_ms(key, value, ttl_ms);
            } else {
                ctx.db.set(key, value);
            }
            append_simple_response(out, "OK");
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
    }
}
