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

pub fn cmd_hello(args: &[&[u8]], ctx: &AppContext, out: &mut Vec<u8>) -> Result<(), String> {
    // HELLO [protover [AUTH username password] [SETNAME clientname]]
    // Without args or protover=2: RESP2 format (array of alternating key-value pairs)
    let protover = args
        .first()
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
    let mode = if ctx.config.is_replica() {
        b"replica".as_slice()
    } else {
        b"standalone".as_slice()
    };
    let role = if ctx.config.is_replica() {
        b"slave".as_slice()
    } else {
        b"master".as_slice()
    };
    append_bulk_response(out, Some(b"mode"));
    append_bulk_response(out, Some(mode));
    append_bulk_response(out, Some(b"role"));
    append_bulk_response(out, Some(role));
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::app_context::AppContext;
    use crate::config::default_config;
    use crate::replication::ReplicationManager;
    use crate::storage::{DashMapStorageEngine, DbConfig, StorageEngine};

    use super::{cmd_auth, cmd_echo, cmd_hello, cmd_ping};

    fn test_ctx() -> AppContext {
        let db: Arc<dyn StorageEngine + Send + Sync> = Arc::new(
            DashMapStorageEngine::new(DbConfig { worker_count: 2 }).expect("db init should work"),
        );
        AppContext::new(
            default_config(),
            db,
            None,
            None,
            None,
            Arc::new(ReplicationManager::new()),
        )
    }

    #[test]
    fn auth_returns_ok_when_requirepass_not_configured() {
        let ctx = test_ctx();
        let mut out = Vec::new();
        cmd_auth(&[b"anything"], &ctx, &mut out).expect("auth should succeed");
        assert_eq!(out, b"+OK\r\n");
    }

    #[test]
    fn auth_validates_password_when_requirepass_is_set() {
        let mut cfg = default_config();
        cfg.requirepass = Some("secret".to_string());
        let db: Arc<dyn StorageEngine + Send + Sync> = Arc::new(
            DashMapStorageEngine::new(DbConfig { worker_count: 2 }).expect("db init should work"),
        );
        let ctx = AppContext::new(
            cfg,
            db,
            None,
            None,
            None,
            Arc::new(ReplicationManager::new()),
        );

        let mut out = Vec::new();
        let err = cmd_auth(&[b"wrong"], &ctx, &mut out).expect_err("wrong password");
        assert_eq!(err, "ERR invalid password");

        out.clear();
        cmd_auth(&[b"secret"], &ctx, &mut out).expect("correct password");
        assert_eq!(out, b"+OK\r\n");
    }

    #[test]
    fn ping_and_echo_return_expected_resp_payloads() {
        let ctx = test_ctx();
        let mut out = Vec::new();
        cmd_ping(&[], &ctx, &mut out).expect("ping without args");
        assert_eq!(out, b"+PONG\r\n");

        out.clear();
        cmd_ping(&[b"hello"], &ctx, &mut out).expect("ping with msg");
        assert_eq!(out, b"$5\r\nhello\r\n");

        out.clear();
        cmd_echo(&[b"world"], &ctx, &mut out).expect("echo");
        assert_eq!(out, b"$5\r\nworld\r\n");
    }

    #[test]
    fn hello_rejects_resp3_and_returns_resp2_fields() {
        let ctx = test_ctx();
        let mut out = Vec::new();
        let err = cmd_hello(&[b"3"], &ctx, &mut out).expect_err("resp3 unsupported");
        assert_eq!(err, "ERR Protocol version 3 is not supported");

        out.clear();
        cmd_hello(&[], &ctx, &mut out).expect("hello resp2");
        assert!(out.starts_with(b"*14\r\n"));
        assert!(out.windows(b"server".len()).any(|w| w == b"server"));
        assert!(out.windows(b"foxkv".len()).any(|w| w == b"foxkv"));
    }
}
