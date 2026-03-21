pub fn required_arg<'a>(args: &[&'a [u8]], index: usize) -> Result<&'a [u8], String> {
    if index < args.len() {
        Ok(args[index])
    } else {
        Err("ERR syntax error".to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::required_arg;

    #[test]
    fn required_arg_returns_item_when_index_is_in_range() {
        let args: Vec<&[u8]> = vec![b"set", b"k", b"v"];
        let got = required_arg(&args, 1).expect("arg should exist");
        assert_eq!(got, b"k");
    }

    #[test]
    fn required_arg_returns_error_when_index_is_out_of_range() {
        let args: Vec<&[u8]> = vec![b"get"];
        let err = required_arg(&args, 2).expect_err("should fail");
        assert_eq!(err, "ERR syntax error");
    }
}
