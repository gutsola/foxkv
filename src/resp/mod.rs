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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_ascii_u64_parses_positive_integer() {
        let result = parse_ascii_u64(b"123").unwrap();
        assert_eq!(result, 123);
    }

    #[test]
    fn parse_ascii_u64_parses_zero() {
        let result = parse_ascii_u64(b"0").unwrap();
        assert_eq!(result, 0);
    }

    #[test]
    fn parse_ascii_u64_parses_large_number() {
        let result = parse_ascii_u64(b"18446744073709551615").unwrap();
        assert_eq!(result, u64::MAX);
    }

    #[test]
    fn parse_ascii_u64_accepts_plus_sign() {
        let result = parse_ascii_u64(b"+456").unwrap();
        assert_eq!(result, 456);
    }

    #[test]
    fn parse_ascii_u64_returns_error_for_empty_input() {
        let result = parse_ascii_u64(b"");
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("invalid number"));
    }

    #[test]
    fn parse_ascii_u64_returns_error_for_plus_only() {
        let result = parse_ascii_u64(b"+");
        assert!(result.is_err());
    }

    #[test]
    fn parse_ascii_u64_returns_error_for_non_digit() {
        let result = parse_ascii_u64(b"12a3");
        assert!(result.is_err());
    }

    #[test]
    fn parse_ascii_u64_returns_error_for_negative_sign() {
        let result = parse_ascii_u64(b"-123");
        assert!(result.is_err());
    }

    #[test]
    fn parse_ascii_u64_returns_error_for_overflow() {
        let result = parse_ascii_u64(b"18446744073709551616");
        assert!(result.is_err());
    }

    #[test]
    fn append_simple_response_produces_correct_format() {
        let mut out = Vec::new();
        append_simple_response(&mut out, "OK");
        assert_eq!(out, b"+OK\r\n");
    }

    #[test]
    fn append_simple_response_handles_empty_string() {
        let mut out = Vec::new();
        append_simple_response(&mut out, "");
        assert_eq!(out, b"+\r\n");
    }

    #[test]
    fn append_simple_response_handles_special_characters() {
        let mut out = Vec::new();
        append_simple_response(&mut out, "Hello World!");
        assert_eq!(out, b"+Hello World!\r\n");
    }

    #[test]
    fn append_error_response_produces_correct_format() {
        let mut out = Vec::new();
        append_error_response(&mut out, "ERR unknown command");
        assert_eq!(out, b"-ERR unknown command\r\n");
    }

    #[test]
    fn append_error_response_handles_empty_message() {
        let mut out = Vec::new();
        append_error_response(&mut out, "");
        assert_eq!(out, b"-\r\n");
    }

    #[test]
    fn append_bulk_response_produces_correct_format_for_value() {
        let mut out = Vec::new();
        append_bulk_response(&mut out, Some(b"hello"));
        assert_eq!(out, b"$5\r\nhello\r\n");
    }

    #[test]
    fn append_bulk_response_produces_null_bulk_for_none() {
        let mut out = Vec::new();
        append_bulk_response(&mut out, None);
        assert_eq!(out, b"$-1\r\n");
    }

    #[test]
    fn append_bulk_response_handles_empty_value() {
        let mut out = Vec::new();
        append_bulk_response(&mut out, Some(b""));
        assert_eq!(out, b"$0\r\n\r\n");
    }

    #[test]
    fn append_bulk_response_handles_binary_data() {
        let mut out = Vec::new();
        append_bulk_response(&mut out, Some(b"\x00\x01\x02"));
        assert_eq!(out, b"$3\r\n\x00\x01\x02\r\n");
    }

    #[test]
    fn append_bulk_response_handles_long_value() {
        let mut out = Vec::new();
        let long_value = vec![b'x'; 1000];
        append_bulk_response(&mut out, Some(&long_value));
        assert!(out.starts_with(b"$1000\r\n"));
        assert!(out.ends_with(b"\r\n"));
    }

    #[test]
    fn append_integer_response_produces_correct_format_for_positive() {
        let mut out = Vec::new();
        append_integer_response(&mut out, 42);
        assert_eq!(out, b":42\r\n");
    }

    #[test]
    fn append_integer_response_produces_correct_format_for_negative() {
        let mut out = Vec::new();
        append_integer_response(&mut out, -123);
        assert_eq!(out, b":-123\r\n");
    }

    #[test]
    fn append_integer_response_produces_correct_format_for_zero() {
        let mut out = Vec::new();
        append_integer_response(&mut out, 0);
        assert_eq!(out, b":0\r\n");
    }

    #[test]
    fn append_integer_response_produces_correct_format_for_large_positive() {
        let mut out = Vec::new();
        append_integer_response(&mut out, 9223372036854775807);
        assert_eq!(out, b":9223372036854775807\r\n");
    }

    #[test]
    fn append_integer_response_produces_correct_format_for_large_negative() {
        let mut out = Vec::new();
        append_integer_response(&mut out, -9223372036854775808);
        assert_eq!(out, b":-9223372036854775808\r\n");
    }

    #[test]
    fn append_usize_ascii_appends_zero() {
        let mut out = Vec::new();
        append_usize_ascii(&mut out, 0);
        assert_eq!(out, b"0");
    }

    #[test]
    fn append_usize_ascii_appends_single_digit() {
        let mut out = Vec::new();
        append_usize_ascii(&mut out, 5);
        assert_eq!(out, b"5");
    }

    #[test]
    fn append_usize_ascii_appends_multiple_digits() {
        let mut out = Vec::new();
        append_usize_ascii(&mut out, 12345);
        assert_eq!(out, b"12345");
    }

    #[test]
    fn append_usize_ascii_appends_max_usize() {
        let mut out = Vec::new();
        append_usize_ascii(&mut out, usize::MAX);
        let expected = usize::MAX.to_string();
        assert_eq!(out, expected.as_bytes());
    }

    #[test]
    fn multiple_appends_accumulate_correctly() {
        let mut out = Vec::new();
        append_simple_response(&mut out, "OK");
        append_integer_response(&mut out, 42);
        append_bulk_response(&mut out, Some(b"value"));
        assert_eq!(out, b"+OK\r\n:42\r\n$5\r\nvalue\r\n");
    }
}
