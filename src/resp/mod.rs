use itoa::Buffer;

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
    let mut buf = Buffer::new();
    out.extend_from_slice(buf.format(value).as_bytes());
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
