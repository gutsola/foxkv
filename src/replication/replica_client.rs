use std::io;
use std::sync::Arc;
use std::time::Duration;

use bytes::BytesMut;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::task;

use crate::app_context::AppContext;
use crate::command::{execute_replication_argv_command, parse_argv_frame};
use crate::config::model::ReplicationConfig;
use crate::persistence::rdb;

#[derive(Debug, Default, Clone)]
struct UpstreamState {
    replid: Option<String>,
    offset: i64,
}

pub fn start_replica_sync_task(ctx: Arc<AppContext>) {
    tokio::spawn(async move {
        let mut backoff_secs = 1_u64;
        let mut state = UpstreamState::default();
        loop {
            match run_sync_session(ctx.clone(), &mut state).await {
                Ok(()) => {
                    backoff_secs = 1;
                }
                Err(err) => {
                    eprintln!("# replica sync disconnected: {err}");
                    tokio::time::sleep(Duration::from_secs(backoff_secs)).await;
                    backoff_secs = (backoff_secs.saturating_mul(2)).min(30);
                }
            }
        }
    });
}

async fn run_sync_session(ctx: Arc<AppContext>, state: &mut UpstreamState) -> io::Result<()> {
    let (master_host, master_port) = match &ctx.config.replication {
        ReplicationConfig::Replica { host, port } => (host.clone(), *port),
        ReplicationConfig::Master => return Ok(()),
    };

    let mut stream = TcpStream::connect((master_host.as_str(), master_port)).await?;
    let _ = stream.set_nodelay(true);

    send_command(&mut stream, &[b"PING"]).await?;
    expect_simple_ok_or_pong(&mut stream).await?;

    let listening_port = ctx.config.port.to_string();
    send_command(
        &mut stream,
        &[b"REPLCONF", b"listening-port", listening_port.as_bytes()],
    )
    .await?;
    expect_simple_ok_or_pong(&mut stream).await?;

    send_command(&mut stream, &[b"REPLCONF", b"capa", b"psync2"]).await?;
    expect_simple_ok_or_pong(&mut stream).await?;

    let replid = state.replid.as_deref().unwrap_or("?");
    let offset_text = if state.replid.is_some() {
        state.offset.to_string()
    } else {
        "-1".to_string()
    };
    send_command(
        &mut stream,
        &[b"PSYNC", replid.as_bytes(), offset_text.as_bytes()],
    )
    .await?;

    let psync_reply = read_line(&mut stream).await?;
    if psync_reply.starts_with(b"+FULLRESYNC ") {
        let reply = std::str::from_utf8(&psync_reply).unwrap_or_default();
        let parts: Vec<&str> = reply.trim().split_whitespace().collect();
        if parts.len() < 3 {
            return Err(io::Error::other("invalid FULLRESYNC reply"));
        }
        state.replid = Some(parts[1].to_string());
        state.offset = parts[2].parse::<i64>().unwrap_or(0);

        let snapshot = read_bulk_payload(&mut stream).await?;
        let db = Arc::clone(&ctx.db);
        task::spawn_blocking(move || {
            db.flush_all();
            rdb::load_from_bytes(db.as_ref(), &snapshot)?;
            Ok::<(), io::Error>(())
        })
        .await
        .map_err(|e| io::Error::other(format!("replica snapshot task failed: {e}")))??;
    } else if psync_reply.starts_with(b"+CONTINUE") {
        if state.offset < 0 {
            state.offset = 0;
        }
    } else {
        let line = String::from_utf8_lossy(&psync_reply);
        return Err(io::Error::other(format!("unexpected PSYNC reply: {line}")));
    }

    apply_replication_stream(&mut stream, ctx, state).await
}

async fn apply_replication_stream(
    stream: &mut TcpStream,
    ctx: Arc<AppContext>,
    state: &mut UpstreamState,
) -> io::Result<()> {
    let mut buffer = BytesMut::with_capacity(16 * 1024);
    let mut response_sink = Vec::with_capacity(256);
    let mut ack_interval = tokio::time::interval(Duration::from_secs(1));
    ack_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    loop {
        loop {
            let consumed = {
                let parsed = parse_argv_frame(buffer.as_ref());
                let (argv, consumed) = match parsed {
                    Some(v) => v,
                    None => break,
                };
                response_sink.clear();
                execute_replication_argv_command(&argv, ctx.as_ref(), &mut response_sink)
                    .map_err(io::Error::other)?;
                consumed
            };
            let _ = buffer.split_to(consumed);
            state.offset = state.offset.saturating_add(consumed as i64);
        }

        tokio::select! {
            n = stream.read_buf(&mut buffer) => {
                let n = n?;
                if n == 0 {
                    return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "master closed replica link"));
                }
            }
            _ = ack_interval.tick() => {
                let ack = state.offset.max(0) as u64;
                let ack_text = ack.to_string();
                send_command(stream, &[b"REPLCONF", b"ACK", ack_text.as_bytes()]).await?;
            }
        }
    }
}

async fn send_command(stream: &mut TcpStream, argv: &[&[u8]]) -> io::Result<()> {
    let mut out = Vec::with_capacity(64);
    out.push(b'*');
    out.extend_from_slice(argv.len().to_string().as_bytes());
    out.extend_from_slice(b"\r\n");
    for arg in argv {
        out.push(b'$');
        out.extend_from_slice(arg.len().to_string().as_bytes());
        out.extend_from_slice(b"\r\n");
        out.extend_from_slice(arg);
        out.extend_from_slice(b"\r\n");
    }
    stream.write_all(&out).await
}

async fn expect_simple_ok_or_pong(stream: &mut TcpStream) -> io::Result<()> {
    let line = read_line(stream).await?;
    if line.starts_with(b"+OK") || line.starts_with(b"+PONG") {
        return Ok(());
    }
    let msg = String::from_utf8_lossy(&line);
    Err(io::Error::other(format!("unexpected handshake reply: {msg}")))
}

async fn read_line(stream: &mut TcpStream) -> io::Result<Vec<u8>> {
    let mut out = Vec::with_capacity(128);
    let mut byte = [0_u8; 1];
    loop {
        stream.read_exact(&mut byte).await?;
        out.push(byte[0]);
        let len = out.len();
        if len >= 2 && out[len - 2] == b'\r' && out[len - 1] == b'\n' {
            out.truncate(len - 2);
            return Ok(out);
        }
    }
}

async fn read_bulk_payload(stream: &mut TcpStream) -> io::Result<Vec<u8>> {
    let line = read_line(stream).await?;
    if !line.starts_with(b"$") {
        let msg = String::from_utf8_lossy(&line);
        return Err(io::Error::other(format!(
            "expected bulk payload length, got '{msg}'"
        )));
    }
    let len = std::str::from_utf8(&line[1..])
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
        .ok_or_else(|| io::Error::other("invalid bulk payload length"))?;
    let mut payload = vec![0_u8; len];
    stream.read_exact(&mut payload).await?;
    let mut crlf = [0_u8; 2];
    stream.read_exact(&mut crlf).await?;
    if crlf != [b'\r', b'\n'] {
        return Err(io::Error::other("invalid bulk payload terminator"));
    }
    Ok(payload)
}
