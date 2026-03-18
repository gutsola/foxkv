use crate::command::SetCondition;

pub fn encode_command(cmd: &[u8], args: &[&[u8]]) -> Vec<u8> {
    let mut total_len = cmd.len();
    for arg in args {
        total_len += arg.len();
    }
    let mut out = Vec::with_capacity(32 + total_len);
    append_array_header(&mut out, 1 + args.len());
    append_bulk(&mut out, cmd);
    for arg in args {
        append_bulk(&mut out, arg);
    }
    out
}

pub fn encode_pairs_command(cmd: &[u8], pairs: &[(&[u8], &[u8])]) -> Vec<u8> {
    let mut args = Vec::with_capacity(pairs.len() * 2);
    for (key, value) in pairs {
        args.push(*key);
        args.push(*value);
    }
    encode_command(cmd, &args)
}

pub fn encode_set_command(
    key: &[u8],
    value: &[u8],
    ttl_ms: Option<u64>,
    condition: SetCondition,
) -> Vec<u8> {
    let mut args = Vec::with_capacity(4);
    args.push(key);
    args.push(value);

    let ttl_buf = ttl_ms.map(|v| v.to_string());
    if let Some(ttl) = ttl_buf.as_ref() {
        args.push(b"PX");
        args.push(ttl.as_bytes());
    }

    match condition {
        SetCondition::None => {}
        SetCondition::Nx => args.push(b"NX"),
        SetCondition::Xx => args.push(b"XX"),
    }

    encode_command(b"SET", &args)
}

fn append_array_header(out: &mut Vec<u8>, len: usize) {
    out.push(b'*');
    out.extend_from_slice(len.to_string().as_bytes());
    out.extend_from_slice(b"\r\n");
}

fn append_bulk(out: &mut Vec<u8>, value: &[u8]) {
    out.push(b'$');
    out.extend_from_slice(value.len().to_string().as_bytes());
    out.extend_from_slice(b"\r\n");
    out.extend_from_slice(value);
    out.extend_from_slice(b"\r\n");
}
