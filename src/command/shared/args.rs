pub fn required_arg<'a>(args: &[&'a [u8]], index: usize) -> Result<&'a [u8], String> {
    if index < args.len() {
        Ok(args[index])
    } else {
        Err("ERR syntax error".to_string())
    }
}
