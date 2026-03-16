use std::io;
use std::str;

use memchr::memchr;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

use crate::storage::ConcurrentMapDb;

pub async fn run_redis_server(addr: &str, db: ConcurrentMapDb) -> io::Result<()> {
    let listener = TcpListener::bind(addr).await?;
    loop {
        let (stream, _) = listener.accept().await?;
        if let Err(err) = stream.set_nodelay(true) {
            eprintln!("set_nodelay failed: {err}");
        }
        let db = db.clone();
        tokio::spawn(async move {
            if let Err(err) = handle_connection(stream, db).await {
                eprintln!("connection error: {err}");
            }
        });
    }
}

const READ_BUF_SIZE: usize = 16 * 1024;

async fn handle_connection(mut stream: TcpStream, db: ConcurrentMapDb) -> io::Result<()> {
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

            match command {
                Command::Set(key, value) => {
                    db.set(key, value);
                    append_simple_response(&mut response_buf, "OK");
                }
                Command::Get(key) => {
                    let value = db.get(key);
                    append_bulk_response(&mut response_buf, value.as_deref());
                }
            }

            read_pos += consumed;
        }

        if !response_buf.is_empty() {
            stream.write_all(&response_buf).await?;
        }
    }
}

enum Command<'a> {
    Get(&'a [u8]),
    Set(&'a [u8], &'a [u8]),
}

fn parse_command(input: &[u8]) -> Result<Option<(Command<'_>, usize)>, String> {
    if input.is_empty() {
        return Ok(None);
    }
    if input[0] != b'*' {
        return Err("ERR Protocol error: expected RESP array".to_string());
    }
    parse_resp_array(input)
}

fn parse_resp_array(input: &[u8]) -> Result<Option<(Command<'_>, usize)>, String> {
    let (array_len, mut cursor) = match parse_number_line(input, b'*')? {
        Some(v) => v,
        None => return Ok(None),
    };
    if array_len <= 0 {
        return Err("ERR Protocol error: invalid multibulk length".to_string());
    }

    let (cmd, next_cursor) = match parse_bulk(input, cursor)? {
        Some(v) => v,
        None => return Ok(None),
    };
    cursor = next_cursor;

    if cmd.eq_ignore_ascii_case(b"set") {
        if array_len != 3 {
            return Err("ERR wrong number of arguments for 'set' command".to_string());
        }
        let (key, next_cursor) = match parse_bulk(input, cursor)? {
            Some(v) => v,
            None => return Ok(None),
        };
        let (value, next_cursor) = match parse_bulk(input, next_cursor)? {
            Some(v) => v,
            None => return Ok(None),
        };
        return Ok(Some((Command::Set(key, value), next_cursor)));
    }

    if cmd.eq_ignore_ascii_case(b"get") {
        if array_len != 2 {
            return Err("ERR wrong number of arguments for 'get' command".to_string());
        }
        let (key, next_cursor) = match parse_bulk(input, cursor)? {
            Some(v) => v,
            None => return Ok(None),
        };
        return Ok(Some((Command::Get(key), next_cursor)));
    }

    let name = String::from_utf8_lossy(cmd);
    Err(format!("ERR unknown command '{}'", name))
}

fn parse_number_line(input: &[u8], prefix: u8) -> Result<Option<(isize, usize)>, String> {
    if input.is_empty() || input[0] != prefix {
        return Err("ERR Protocol error: invalid prefix".to_string());
    }
    let rel_pos = match find_crlf(&input[1..]) {
        Some(v) => v,
        None => return Ok(None),
    };
    let end = 1 + rel_pos;
    let value = str::from_utf8(&input[1..end])
        .map_err(|_| "ERR Protocol error: invalid number".to_string())?
        .parse::<isize>()
        .map_err(|_| "ERR Protocol error: invalid number".to_string())?;
    Ok(Some((value, end + 2)))
}

fn find_crlf(input: &[u8]) -> Option<usize> {
    let mut offset = 0_usize;
    while let Some(rel_lf) = memchr(b'\n', &input[offset..]) {
        let lf = offset + rel_lf;
        if lf > 0 && input[lf - 1] == b'\r' {
            return Some(lf - 1);
        }
        offset = lf + 1;
    }
    None
}

fn parse_bulk(input: &[u8], cursor: usize) -> Result<Option<(&[u8], usize)>, String> {
    if cursor >= input.len() {
        return Ok(None);
    }
    if input[cursor] != b'$' {
        return Err("ERR Protocol error: expected '$'".to_string());
    }
    let (bulk_len, next_cursor) = match parse_number_line(&input[cursor..], b'$')? {
        Some(v) => v,
        None => return Ok(None),
    };
    if bulk_len < 0 {
        return Err("ERR Protocol error: invalid bulk length".to_string());
    }
    let data_start = cursor + next_cursor;
    let bulk_len = bulk_len as usize;
    if input.len() < data_start + bulk_len + 2 {
        return Ok(None);
    }
    if &input[data_start + bulk_len..data_start + bulk_len + 2] != b"\r\n" {
        return Err("ERR Protocol error: expected CRLF after bulk".to_string());
    }
    let data = &input[data_start..data_start + bulk_len];
    Ok(Some((data, data_start + bulk_len + 2)))
}

fn append_simple_response(out: &mut Vec<u8>, msg: &str) {
    out.push(b'+');
    out.extend_from_slice(msg.as_bytes());
    out.extend_from_slice(b"\r\n");
}

fn append_error_response(out: &mut Vec<u8>, message: &str) {
    out.push(b'-');
    out.extend_from_slice(message.as_bytes());
    out.extend_from_slice(b"\r\n");
}

fn append_bulk_response(out: &mut Vec<u8>, value: Option<&[u8]>) {
    if let Some(value) = value {
        out.push(b'$');
        append_usize_ascii(out, value.len());
        out.extend_from_slice(b"\r\n");
        out.extend_from_slice(value);
        out.extend_from_slice(b"\r\n");
    } else {
        out.extend_from_slice(b"$-1\r\n");
    }
}

fn append_usize_ascii(out: &mut Vec<u8>, mut n: usize) {
    if n == 0 {
        out.push(b'0');
        return;
    }
    let mut digits = [0_u8; 20];
    let mut idx = digits.len();
    while n > 0 {
        idx -= 1;
        digits[idx] = b'0' + (n % 10) as u8;
        n /= 10;
    }
    out.extend_from_slice(&digits[idx..]);
}
