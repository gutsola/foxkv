use std::io;
use std::sync::Arc;

use bytes::BytesMut;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::broadcast::error::RecvError;
use tokio::task;

use crate::app_context::AppContext;
use crate::command::{ExecTransition, execute_argv_command, parse_argv_frame};
use crate::persistence::rdb;
use crate::resp::append_error_response;

const READ_BUF_SIZE: usize = 16 * 1024;

pub async fn handle_connection(mut stream: TcpStream, ctx: Arc<AppContext>) -> io::Result<()> {
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
