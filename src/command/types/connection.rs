use crate::app_context::AppContext;
use crate::command::shared::args::required_arg;
use crate::command::shared::wire::append_array_header;
use crate::resp::{append_bulk_response, append_integer_response, append_simple_response};

macro_rules! connection_commands {
    ($m:ident) => {
        $m!(connection, auth, cmd_auth);
        $m!(connection, echo, cmd_echo);
        $m!(connection, hello, cmd_hello);
        $m!(connection, ping, cmd_ping);
    };
}
pub(crate) use connection_commands;

const SERVER_VERSION: &str = env!("CARGO_PKG_VERSION");

pub fn cmd_auth(args: &[&[u8]], ctx: &AppContext, out: &mut Vec<u8>) -> Result<(), String> {
    let password = required_arg(args, 0)?;
    match &ctx.config.requirepass {
        None => {
            // No password configured: always succeed (Redis behavior when requirepass is not set)
            append_simple_response(out, "OK");
        }
        Some(configured) => {
            if password != configured.as_bytes() {
                return Err("ERR invalid password".to_string());
            }
            append_simple_response(out, "OK");
        }
    }
    Ok(())
}

pub fn cmd_echo(args: &[&[u8]], _ctx: &AppContext, out: &mut Vec<u8>) -> Result<(), String> {
    let message = required_arg(args, 0)?;
    append_bulk_response(out, Some(message));
    Ok(())
}

pub fn cmd_hello(args: &[&[u8]], _ctx: &AppContext, out: &mut Vec<u8>) -> Result<(), String> {
    // HELLO [protover [AUTH username password] [SETNAME clientname]]
    // Without args or protover=2: RESP2 format (array of alternating key-value pairs)
    let protover = args
        .get(0)
        .and_then(|v| std::str::from_utf8(v).ok())
        .and_then(|s| s.parse::<u32>().ok());
    if let Some(3) = protover {
        return Err("ERR Protocol version 3 is not supported".to_string());
    }
    // RESP2: *14 for 7 key-value pairs (server, version, proto, id, mode, role, modules)
    append_array_header(out, 14);
    append_bulk_response(out, Some(b"server"));
    append_bulk_response(out, Some(b"foxkv"));
    append_bulk_response(out, Some(b"version"));
    append_bulk_response(out, Some(SERVER_VERSION.as_bytes()));
    append_bulk_response(out, Some(b"proto"));
    append_integer_response(out, 2);
    append_bulk_response(out, Some(b"id"));
    append_integer_response(out, 0);
    append_bulk_response(out, Some(b"mode"));
    append_bulk_response(out, Some(b"standalone"));
    append_bulk_response(out, Some(b"role"));
    append_bulk_response(out, Some(b"master"));
    append_bulk_response(out, Some(b"modules"));
    out.extend_from_slice(b"*0\r\n");
    Ok(())
}

pub fn cmd_ping(args: &[&[u8]], _ctx: &AppContext, out: &mut Vec<u8>) -> Result<(), String> {
    if args.is_empty() {
        append_simple_response(out, "PONG");
    } else {
        let message = required_arg(args, 0)?;
        append_bulk_response(out, Some(message));
    }
    Ok(())
}
