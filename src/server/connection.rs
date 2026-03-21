use std::io;
use std::net::SocketAddr;
use std::sync::Arc;

use bytes::BytesMut;
use log::debug;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::broadcast::error::RecvError;
use tokio::task;

use crate::app_context::AppContext;
use crate::command::{ExecTransition, execute_argv_command, parse_argv_frame};
use crate::persistence::rdb;
use crate::resp::append_error_response;

const READ_BUF_SIZE: usize = 16 * 1024;
const MAX_DEBUG_VALUE_LEN: usize = 64;

pub async fn handle_connection(
    mut stream: TcpStream,
    peer_addr: SocketAddr,
    ctx: Arc<AppContext>,
) -> io::Result<()> {
    let mut buffer = BytesMut::with_capacity(4096);
    let mut response_buf = Vec::with_capacity(READ_BUF_SIZE);

    loop {
        let n = stream.read_buf(&mut buffer).await?;
        if n == 0 {
            return Ok(());
        }
        response_buf.clear();

        loop {
            let consumed = {
                let parsed = parse_argv_frame(buffer.as_ref());
                let (argv, consumed) = match parsed {
                    Some(value) => value,
                    None => break,
                };
                debug!("request from {}: {}", peer_addr, summarize_argv(&argv));
                match execute_argv_command(&argv, ctx.as_ref(), &mut response_buf) {
                    Ok(outcome) => {
                        if let ExecTransition::EnterReplicaStream {
                            start_offset,
                            send_empty_rdb,
                        } = outcome.transition
                        {
                            return enter_replica_stream(
                                stream,
                                ctx,
                                &response_buf,
                                start_offset,
                                send_empty_rdb,
                            )
                            .await;
                        }
                    }
                    Err(err) => append_error_response(&mut response_buf, &err),
                }
                consumed
            };
            let _ = buffer.split_to(consumed);
        }

        if !response_buf.is_empty() {
            debug!(
                "response to {}: bytes={}, {}",
                peer_addr,
                response_buf.len(),
                summarize_response(&response_buf)
            );
            stream.write_all(&response_buf).await?;
        }
    }
}

async fn enter_replica_stream(
    mut stream: TcpStream,
    ctx: Arc<AppContext>,
    handshake_reply: &[u8],
    start_offset: u64,
    send_empty_rdb: bool,
) -> io::Result<()> {
    if !handshake_reply.is_empty() {
        stream.write_all(handshake_reply).await?;
    }
    if send_empty_rdb {
        let db = Arc::clone(&ctx.db);
        let with_checksum = ctx.config.rdb.rdbchecksum;
        let rdb_payload = task::spawn_blocking(move || {
            rdb::build_rdb_snapshot_bytes(db.as_ref(), with_checksum)
        })
        .await
        .map_err(|err| io::Error::other(format!("rdb snapshot task failed: {err}")))??;
        stream
            .write_all(format!("${}\r\n", rdb_payload.len()).as_bytes())
            .await?;
        stream.write_all(&rdb_payload).await?;
        stream.write_all(b"\r\n").await?;
    }

    let mut subscription = ctx.replication.subscribe_from(start_offset);
    for entry in subscription.history.drain(..) {
        stream.write_all(&entry).await?;
    }

    loop {
        match subscription.receiver.recv().await {
            Ok(event) => {
                if event.end_offset < start_offset {
                    continue;
                }
                stream.write_all(&event.payload).await?;
            }
            Err(RecvError::Lagged(_)) => {
                // Replica is too slow, close link and let it PSYNC again.
                return Ok(());
            }
            Err(RecvError::Closed) => return Ok(()),
        }
    }
}

fn summarize_argv(argv: &[&[u8]]) -> String {
    let Some(cmd_raw) = argv.first() else {
        return "<empty argv>".to_string();
    };
    let cmd = ascii_uppercase_lossy(cmd_raw);
    let mut parts = Vec::with_capacity(argv.len().saturating_sub(1));
    for (idx, arg) in argv.iter().enumerate().skip(1) {
        if should_redact_arg(&cmd, idx) {
            parts.push("<redacted>".to_string());
        } else {
            parts.push(preview_bytes(arg));
        }
    }
    if parts.is_empty() {
        cmd
    } else {
        format!("{} {}", cmd, parts.join(" "))
    }
}

fn summarize_response(out: &[u8]) -> String {
    if out.is_empty() {
        return "<empty>".to_string();
    }
    let end = out
        .windows(2)
        .position(|w| w == b"\r\n")
        .unwrap_or(out.len().saturating_sub(1));
    let line = if end < out.len() {
        &out[..end]
    } else {
        out
    };
    preview_bytes(line)
}

fn ascii_uppercase_lossy(raw: &[u8]) -> String {
    String::from_utf8_lossy(raw).to_ascii_uppercase()
}

fn should_redact_arg(cmd_upper: &str, arg_index: usize) -> bool {
    matches!(cmd_upper, "AUTH" | "HELLO" | "CONFIG" | "ACL")
        || (cmd_upper == "REPLCONF" && arg_index >= 2)
}

fn preview_bytes(raw: &[u8]) -> String {
    let mut out = String::new();
    for &b in raw.iter().take(MAX_DEBUG_VALUE_LEN) {
        if b.is_ascii_graphic() || b == b' ' {
            out.push(char::from(b));
        } else {
            out.push('.');
        }
    }
    if raw.len() > MAX_DEBUG_VALUE_LEN {
        out.push_str("...");
    }
    out
}
