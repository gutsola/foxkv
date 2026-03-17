use std::io;
use std::sync::Arc;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use crate::app_context::AppContext;
use crate::command::exec::execute_command;
use crate::command::parse_command;
use crate::resp::append_error_response;

const READ_BUF_SIZE: usize = 16 * 1024;

pub async fn handle_connection(mut stream: TcpStream, ctx: Arc<AppContext>) -> io::Result<()> {
    let mut read_buf = [0_u8; READ_BUF_SIZE];
    let mut buffer = Vec::with_capacity(4096);
    let mut read_pos = 0_usize;
    let mut response_buf = Vec::with_capacity(READ_BUF_SIZE);

    loop {
        let n = stream.read(&mut read_buf).await?;
        if n == 0 {
            return Ok(());
        }
        buffer.extend_from_slice(&read_buf[..n]);
        response_buf.clear();

        loop {
            let parsed = parse_command(&buffer[read_pos..]);
            let (command, consumed) = match parsed {
                Ok(Some(value)) => value,
                Ok(None) => break,
                Err(err) => {
                    append_error_response(&mut response_buf, &err);
                    buffer.clear();
                    read_pos = 0;
                    break;
                }
            };
            if let Err(err) = execute_command(command, ctx.as_ref(), &mut response_buf) {
                append_error_response(&mut response_buf, &err);
            }
            read_pos += consumed;
        }

        if !response_buf.is_empty() {
            stream.write_all(&response_buf).await?;
        }
    }
}
