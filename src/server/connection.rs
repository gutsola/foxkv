use std::io;
use std::sync::Arc;

use bytes::BytesMut;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use crate::app_context::AppContext;
use crate::command::{execute_argv_command, parse_argv_frame};
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
                if let Err(err) = execute_argv_command(&argv, ctx.as_ref(), &mut response_buf) {
                    append_error_response(&mut response_buf, &err);
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
