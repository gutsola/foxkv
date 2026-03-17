pub mod exec;

use crate::resp::{parse_ascii_u64, parse_bulk, parse_number_line};

#[derive(Clone, Copy)]
pub enum SetCondition {
    None,
    Nx,
    Xx,
}

pub enum Command<'a> {
    Ping(Option<&'a [u8]>),
    Get(&'a [u8]),
    Ttl(&'a [u8]),
    DbSize,
    Scan(usize, usize),
    Set(&'a [u8], &'a [u8], Option<u64>, SetCondition),
    SetNx(&'a [u8], &'a [u8]),
    SetEx(&'a [u8], &'a [u8], u64),
    PSetEx(&'a [u8], &'a [u8], u64),
    GetSet(&'a [u8], &'a [u8]),
    MSet(Vec<(&'a [u8], &'a [u8])>),
    MSetNx(Vec<(&'a [u8], &'a [u8])>),
    Del(Vec<&'a [u8]>),
    Exists(Vec<&'a [u8]>),
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
        if array_len < 3 || array_len > 6 {
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
        let (ttl_ms, condition, final_cursor) =
            match parse_set_options(input, next_cursor, array_len - 3)? {
                Some(v) => v,
                None => return Ok(None),
            };
        return Ok(Some((
            Command::Set(key, value, ttl_ms, condition),
            final_cursor,
        )));
    }

    if cmd.eq_ignore_ascii_case(b"setnx") {
        if array_len != 3 {
            return Err("ERR wrong number of arguments for 'setnx' command".to_string());
        }
        let (key, next_cursor) = match parse_bulk(input, cursor)? {
            Some(v) => v,
            None => return Ok(None),
        };
        let (value, next_cursor) = match parse_bulk(input, next_cursor)? {
            Some(v) => v,
            None => return Ok(None),
        };
        return Ok(Some((Command::SetNx(key, value), next_cursor)));
    }

    if cmd.eq_ignore_ascii_case(b"setex") {
        if array_len != 4 {
            return Err("ERR wrong number of arguments for 'setex' command".to_string());
        }
        let (key, next_cursor) = match parse_bulk(input, cursor)? {
            Some(v) => v,
            None => return Ok(None),
        };
        let (ttl_raw, next_cursor) = match parse_bulk(input, next_cursor)? {
            Some(v) => v,
            None => return Ok(None),
        };
        let ttl_ms = parse_ttl_ms(b"ex", ttl_raw)?;
        let (value, next_cursor) = match parse_bulk(input, next_cursor)? {
            Some(v) => v,
            None => return Ok(None),
        };
        return Ok(Some((Command::SetEx(key, value, ttl_ms), next_cursor)));
    }

    if cmd.eq_ignore_ascii_case(b"psetex") {
        if array_len != 4 {
            return Err("ERR wrong number of arguments for 'psetex' command".to_string());
        }
        let (key, next_cursor) = match parse_bulk(input, cursor)? {
            Some(v) => v,
            None => return Ok(None),
        };
        let (ttl_raw, next_cursor) = match parse_bulk(input, next_cursor)? {
            Some(v) => v,
            None => return Ok(None),
        };
        let ttl_ms = parse_ttl_ms(b"px", ttl_raw)?;
        let (value, next_cursor) = match parse_bulk(input, next_cursor)? {
            Some(v) => v,
            None => return Ok(None),
        };
        return Ok(Some((Command::PSetEx(key, value, ttl_ms), next_cursor)));
    }

    if cmd.eq_ignore_ascii_case(b"getset") {
        if array_len != 3 {
            return Err("ERR wrong number of arguments for 'getset' command".to_string());
        }
        let (key, next_cursor) = match parse_bulk(input, cursor)? {
            Some(v) => v,
            None => return Ok(None),
        };
        let (value, next_cursor) = match parse_bulk(input, next_cursor)? {
            Some(v) => v,
            None => return Ok(None),
        };
        return Ok(Some((Command::GetSet(key, value), next_cursor)));
    }

    if cmd.eq_ignore_ascii_case(b"mset") {
        if array_len < 3 || array_len % 2 == 0 {
            return Err("ERR wrong number of arguments for 'mset' command".to_string());
        }
        let (pairs, next_cursor) = match parse_key_value_pairs(input, cursor, (array_len - 1) / 2)?
        {
            Some(v) => v,
            None => return Ok(None),
        };
        return Ok(Some((Command::MSet(pairs), next_cursor)));
    }

    if cmd.eq_ignore_ascii_case(b"msetnx") {
        if array_len < 3 || array_len % 2 == 0 {
            return Err("ERR wrong number of arguments for 'msetnx' command".to_string());
        }
        let (pairs, next_cursor) = match parse_key_value_pairs(input, cursor, (array_len - 1) / 2)?
        {
            Some(v) => v,
            None => return Ok(None),
        };
        return Ok(Some((Command::MSetNx(pairs), next_cursor)));
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

    if cmd.eq_ignore_ascii_case(b"dbsize") {
        if array_len != 1 {
            return Err("ERR wrong number of arguments for 'dbsize' command".to_string());
        }
        return Ok(Some((Command::DbSize, cursor)));
    }

    if cmd.eq_ignore_ascii_case(b"scan") {
        if array_len != 2 && array_len != 4 {
            return Err("ERR wrong number of arguments for 'scan' command".to_string());
        }
        let (cursor_raw, next_cursor) = match parse_bulk(input, cursor)? {
            Some(v) => v,
            None => return Ok(None),
        };
        let cursor_num = parse_ascii_u64(cursor_raw)?;
        let scan_cursor =
            usize::try_from(cursor_num).map_err(|_| "ERR invalid cursor".to_string())?;
        if array_len == 2 {
            return Ok(Some((Command::Scan(scan_cursor, 10), next_cursor)));
        }
        let (option, next_cursor) = match parse_bulk(input, next_cursor)? {
            Some(v) => v,
            None => return Ok(None),
        };
        if !option.eq_ignore_ascii_case(b"count") {
            return Err("ERR syntax error".to_string());
        }
        let (count_raw, next_cursor) = match parse_bulk(input, next_cursor)? {
            Some(v) => v,
            None => return Ok(None),
        };
        let count = parse_ascii_u64(count_raw)?;
        if count == 0 {
            return Err("ERR syntax error".to_string());
        }
        let count = usize::try_from(count).map_err(|_| "ERR syntax error".to_string())?;
        return Ok(Some((Command::Scan(scan_cursor, count), next_cursor)));
    }

    if cmd.eq_ignore_ascii_case(b"del") {
        if array_len < 2 {
            return Err("ERR wrong number of arguments for 'del' command".to_string());
        }
        let (keys, next_cursor) = match parse_keys(input, cursor, array_len - 1)? {
            Some(v) => v,
            None => return Ok(None),
        };
        return Ok(Some((Command::Del(keys), next_cursor)));
    }

    if cmd.eq_ignore_ascii_case(b"exists") {
        if array_len < 2 {
            return Err("ERR wrong number of arguments for 'exists' command".to_string());
        }
        let (keys, next_cursor) = match parse_keys(input, cursor, array_len - 1)? {
            Some(v) => v,
            None => return Ok(None),
        };
        return Ok(Some((Command::Exists(keys), next_cursor)));
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

fn parse_set_options(
    input: &[u8],
    mut cursor: usize,
    remaining_args: isize,
) -> Result<Option<(Option<u64>, SetCondition, usize)>, String> {
    if remaining_args < 0 || remaining_args > 3 {
        return Err("ERR syntax error".to_string());
    }
    let mut ttl_ms = None;
    let mut condition = SetCondition::None;
    let mut parsed = 0_isize;

    while parsed < remaining_args {
        let (arg, next_cursor) = match parse_bulk(input, cursor)? {
            Some(v) => v,
            None => return Ok(None),
        };
        parsed += 1;
        cursor = next_cursor;

        if arg.eq_ignore_ascii_case(b"nx") {
            if !matches!(condition, SetCondition::None) {
                return Err("ERR syntax error".to_string());
            }
            condition = SetCondition::Nx;
            continue;
        }
        if arg.eq_ignore_ascii_case(b"xx") {
            if !matches!(condition, SetCondition::None) {
                return Err("ERR syntax error".to_string());
            }
            condition = SetCondition::Xx;
            continue;
        }
        if arg.eq_ignore_ascii_case(b"ex") || arg.eq_ignore_ascii_case(b"px") {
            if ttl_ms.is_some() || parsed >= remaining_args {
                return Err("ERR syntax error".to_string());
            }
            let (ttl_raw, next_cursor) = match parse_bulk(input, cursor)? {
                Some(v) => v,
                None => return Ok(None),
            };
            ttl_ms = Some(parse_ttl_ms(arg, ttl_raw)?);
            parsed += 1;
            cursor = next_cursor;
            continue;
        }
        return Err("ERR syntax error".to_string());
    }
    Ok(Some((ttl_ms, condition, cursor)))
}

fn parse_keys<'a>(
    input: &'a [u8],
    mut cursor: usize,
    count: isize,
) -> Result<Option<(Vec<&'a [u8]>, usize)>, String> {
    let key_count = usize::try_from(count)
        .map_err(|_| "ERR Protocol error: invalid multibulk length".to_string())?;
    let mut keys = Vec::with_capacity(key_count);
    for _ in 0..key_count {
        let (key, next_cursor) = match parse_bulk(input, cursor)? {
            Some(v) => v,
            None => return Ok(None),
        };
        keys.push(key);
        cursor = next_cursor;
    }
    Ok(Some((keys, cursor)))
}

fn parse_key_value_pairs<'a>(
    input: &'a [u8],
    mut cursor: usize,
    count: isize,
) -> Result<Option<(Vec<(&'a [u8], &'a [u8])>, usize)>, String> {
    let pair_count = usize::try_from(count)
        .map_err(|_| "ERR Protocol error: invalid multibulk length".to_string())?;
    let mut pairs = Vec::with_capacity(pair_count);
    for _ in 0..pair_count {
        let (key, next_cursor) = match parse_bulk(input, cursor)? {
            Some(v) => v,
            None => return Ok(None),
        };
        let (value, next_cursor) = match parse_bulk(input, next_cursor)? {
            Some(v) => v,
            None => return Ok(None),
        };
        pairs.push((key, value));
        cursor = next_cursor;
    }
    Ok(Some((pairs, cursor)))
}
