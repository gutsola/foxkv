pub mod exec;

use crate::resp::{parse_ascii_u64, parse_bulk, parse_number_line};

pub enum Command<'a> {
    Ping(Option<&'a [u8]>),
    Get(&'a [u8]),
    Ttl(&'a [u8]),
    Set(&'a [u8], &'a [u8], Option<u64>),
}

pub fn parse_command(input: &[u8]) -> Result<Option<(Command<'_>, usize)>, String> {
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

    if cmd.eq_ignore_ascii_case(b"ping") {
        if array_len == 1 {
            return Ok(Some((Command::Ping(None), cursor)));
        }
        if array_len == 2 {
            let (message, next_cursor) = match parse_bulk(input, cursor)? {
                Some(v) => v,
                None => return Ok(None),
            };
            return Ok(Some((Command::Ping(Some(message)), next_cursor)));
        }
        return Err("ERR wrong number of arguments for 'ping' command".to_string());
    }

    if cmd.eq_ignore_ascii_case(b"set") {
        if array_len != 3 && array_len != 5 {
            return Err(
                "ERR wrong number of arguments for 'set' command, expected: SET key value [EX seconds|PX milliseconds]"
                    .to_string(),
            );
        }
        let (key, next_cursor) = match parse_bulk(input, cursor)? {
            Some(v) => v,
            None => return Ok(None),
        };
        let (value, mut next_cursor) = match parse_bulk(input, next_cursor)? {
            Some(v) => v,
            None => return Ok(None),
        };
        let ttl_ms = if array_len == 5 {
            let (ttl_kind, after_ttl_kind) = match parse_bulk(input, next_cursor)? {
                Some(v) => v,
                None => return Ok(None),
            };
            let (ttl_raw, after_ttl_value) = match parse_bulk(input, after_ttl_kind)? {
                Some(v) => v,
                None => return Ok(None),
            };
            next_cursor = after_ttl_value;
            Some(parse_ttl_ms(ttl_kind, ttl_raw)?)
        } else {
            None
        };
        return Ok(Some((Command::Set(key, value, ttl_ms), next_cursor)));
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

    if cmd.eq_ignore_ascii_case(b"ttl") {
        if array_len != 2 {
            return Err("ERR wrong number of arguments for 'ttl' command".to_string());
        }
        let (key, next_cursor) = match parse_bulk(input, cursor)? {
            Some(v) => v,
            None => return Ok(None),
        };
        return Ok(Some((Command::Ttl(key), next_cursor)));
    }

    let name = String::from_utf8_lossy(cmd);
    Err(format!("ERR unknown command '{}'", name))
}

fn parse_ttl_ms(ttl_kind: &[u8], ttl_raw: &[u8]) -> Result<u64, String> {
    let ttl_value = parse_ascii_u64(ttl_raw)?;
    if ttl_value == 0 {
        return Err("ERR invalid expire time in 'set' command".to_string());
    }
    if ttl_kind.eq_ignore_ascii_case(b"px") {
        return Ok(ttl_value);
    }
    if ttl_kind.eq_ignore_ascii_case(b"ex") {
        return ttl_value
            .checked_mul(1000)
            .ok_or_else(|| "ERR invalid expire time in 'set' command".to_string());
    }
    Err("ERR syntax error".to_string())
}
