use std::str;

use memchr::memchr;

pub fn parse_number_line(input: &[u8], prefix: u8) -> Result<Option<(isize, usize)>, String> {
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

pub fn parse_bulk(input: &[u8], cursor: usize) -> Result<Option<(&[u8], usize)>, String> {
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

pub fn parse_ascii_u64(input: &[u8]) -> Result<u64, String> {
    if input.is_empty() {
        return Err("ERR Protocol error: invalid number".to_string());
    }
    let mut idx = 0_usize;
    if input[0] == b'+' {
        idx = 1;
    }
    if idx >= input.len() {
        return Err("ERR Protocol error: invalid number".to_string());
    }

    let mut value = 0_u64;
    while idx < input.len() {
        let digit = input[idx];
        if !digit.is_ascii_digit() {
            return Err("ERR Protocol error: invalid number".to_string());
        }
        value = value
            .checked_mul(10)
            .and_then(|v| v.checked_add(u64::from(digit - b'0')))
            .ok_or_else(|| "ERR Protocol error: invalid number".to_string())?;
        idx += 1;
    }
    Ok(value)
}

pub fn append_simple_response(out: &mut Vec<u8>, msg: &str) {
    out.push(b'+');
    out.extend_from_slice(msg.as_bytes());
    out.extend_from_slice(b"\r\n");
}

pub fn append_error_response(out: &mut Vec<u8>, message: &str) {
    out.push(b'-');
    out.extend_from_slice(message.as_bytes());
    out.extend_from_slice(b"\r\n");
}

pub fn append_bulk_response(out: &mut Vec<u8>, value: Option<&[u8]>) {
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

pub fn append_integer_response(out: &mut Vec<u8>, value: i64) {
    out.push(b':');
    out.extend_from_slice(value.to_string().as_bytes());
    out.extend_from_slice(b"\r\n");
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
