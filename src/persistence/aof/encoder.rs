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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encode_command_produces_valid_resp_array() {
        let result = encode_command(b"SET", &[b"key", b"value"]);
        assert_eq!(result, b"*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n");
    }

    #[test]
    fn encode_command_handles_empty_args() {
        let result = encode_command(b"PING", &[]);
        assert_eq!(result, b"*1\r\n$4\r\nPING\r\n");
    }

    #[test]
    fn encode_command_handles_multiple_args() {
        let result = encode_command(b"MSET", &[b"k1", b"v1", b"k2", b"v2"]);
        assert_eq!(result, b"*5\r\n$4\r\nMSET\r\n$2\r\nk1\r\n$2\r\nv1\r\n$2\r\nk2\r\n$2\r\nv2\r\n");
    }

    #[test]
    fn encode_pairs_command_produces_correct_structure() {
        let pairs: &[(&[u8], &[u8])] = &[(b"key1", b"val1"), (b"key2", b"val2")];
        let result = encode_pairs_command(b"MSET", pairs);
        assert_eq!(result, b"*5\r\n$4\r\nMSET\r\n$4\r\nkey1\r\n$4\r\nval1\r\n$4\r\nkey2\r\n$4\r\nval2\r\n");
    }

    #[test]
    fn encode_pairs_command_handles_empty_pairs() {
        let pairs: &[(&[u8], &[u8])] = &[];
        let result = encode_pairs_command(b"MSET", pairs);
        assert_eq!(result, b"*1\r\n$4\r\nMSET\r\n");
    }

    #[test]
    fn encode_set_command_without_ttl_or_condition() {
        let result = encode_set_command(b"mykey", b"myvalue", None, SetCondition::None);
        assert_eq!(result, b"*3\r\n$3\r\nSET\r\n$5\r\nmykey\r\n$7\r\nmyvalue\r\n");
    }

    #[test]
    fn encode_set_command_with_ttl() {
        let result = encode_set_command(b"key", b"value", Some(5000), SetCondition::None);
        assert_eq!(result, b"*5\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n$2\r\nPX\r\n$4\r\n5000\r\n");
    }

    #[test]
    fn encode_set_command_with_nx_condition() {
        let result = encode_set_command(b"key", b"value", None, SetCondition::Nx);
        assert_eq!(result, b"*4\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n$2\r\nNX\r\n");
    }

    #[test]
    fn encode_set_command_with_xx_condition() {
        let result = encode_set_command(b"key", b"value", None, SetCondition::Xx);
        assert_eq!(result, b"*4\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n$2\r\nXX\r\n");
    }

    #[test]
    fn encode_set_command_with_ttl_and_condition() {
        let result = encode_set_command(b"key", b"val", Some(10000), SetCondition::Nx);
        assert_eq!(result, b"*6\r\n$3\r\nSET\r\n$3\r\nkey\r\n$3\r\nval\r\n$2\r\nPX\r\n$5\r\n10000\r\n$2\r\nNX\r\n");
    }

    #[test]
    fn encode_command_handles_binary_data() {
        let result = encode_command(b"SET", &[b"key", b"\x00\x01\x02"]);
        assert_eq!(result, b"*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$3\r\n\x00\x01\x02\r\n");
    }

    #[test]
    fn encode_command_handles_long_values() {
        let long_value = vec![b'x'; 1000];
        let result = encode_command(b"SET", &[b"key", &long_value]);
        assert!(result.starts_with(b"*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$1000\r\n"));
        assert!(result.ends_with(b"\r\n"));
    }
}
