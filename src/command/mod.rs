use std::collections::HashMap;
use std::sync::OnceLock;

mod shared;
pub mod types;

use crate::app_context::AppContext;
use crate::resp::{parse_bulk, parse_number_line};

#[derive(Clone, Copy)]
pub enum SetCondition {
    None,
    Nx,
    Xx,
}

pub type CommandHandler = fn(&[&[u8]], &AppContext, &mut Vec<u8>) -> Result<(), String>;

static REGISTRY: OnceLock<HashMap<String, CommandHandler>> = OnceLock::new();

pub fn parse_argv_frame(input: &[u8]) -> Result<Option<(Vec<&[u8]>, usize)>, String> {
    if input.is_empty() {
        return Ok(None);
    }
    if input[0] != b'*' {
        return Err("ERR Protocol error: expected RESP array".to_string());
    }
    parse_resp_array_to_argv(input)
}

pub fn init_command_registry() {
    let _ = command_registry();
}

pub fn execute_argv_command(
    argv: &[&[u8]],
    ctx: &AppContext,
    out: &mut Vec<u8>,
) -> Result<(), String> {
    let Some((cmd, args)) = argv.split_first() else {
        return Err("ERR Protocol error: empty command".to_string());
    };
    let key = lowercase_ascii(cmd);
    let registry = command_registry();
    let Some(handler) = registry.get(&key) else {
        let name = String::from_utf8_lossy(cmd);
        return Err(format!("ERR unknown command '{}'", name));
    };
    handler(args, ctx, out)
}

fn command_registry() -> &'static HashMap<String, CommandHandler> {
    REGISTRY.get_or_init(|| {
        let mut registry = HashMap::new();
        types::string::register_handlers(&mut registry);
        registry
    })
}

fn parse_resp_array_to_argv(input: &[u8]) -> Result<Option<(Vec<&[u8]>, usize)>, String> {
    let (array_len, mut cursor) = match parse_number_line(input, b'*')? {
        Some(v) => v,
        None => return Ok(None),
    };
    if array_len <= 0 {
        return Err("ERR Protocol error: invalid multibulk length".to_string());
    }
    let arg_count = usize::try_from(array_len)
        .map_err(|_| "ERR Protocol error: invalid multibulk length".to_string())?;
    let mut argv = Vec::with_capacity(arg_count);
    for _ in 0..arg_count {
        let (arg, next_cursor) = match parse_bulk(input, cursor)? {
            Some(v) => v,
            None => return Ok(None),
        };
        argv.push(arg);
        cursor = next_cursor;
    }
    Ok(Some((argv, cursor)))
}

fn lowercase_ascii(input: &[u8]) -> String {
    let mut out = String::with_capacity(input.len());
    for &b in input {
        out.push((if b.is_ascii_uppercase() { b + 32 } else { b }) as char);
    }
    out
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::{execute_argv_command, parse_argv_frame};
    use crate::app_context::AppContext;
    use crate::config::default_config;
    use crate::storage::{DashMapStorageEngine, DbConfig, StorageEngine};

    #[test]
    fn string_set_then_get_dispatch_works() {
        let db: Arc<dyn StorageEngine + Send + Sync> = Arc::new(
            DashMapStorageEngine::new(DbConfig { worker_count: 2 }).expect("db init failed"),
        );
        let ctx = AppContext::new(default_config(), db, None);
        let set = b"*3\r\n$3\r\nSET\r\n$2\r\nk1\r\n$2\r\nv1\r\n";
        let get = b"*2\r\n$3\r\nGET\r\n$2\r\nk1\r\n";

        let (argv1, consumed1) = parse_argv_frame(set)
            .expect("parse set failed")
            .expect("set should be complete");
        assert_eq!(consumed1, set.len());
        let mut out = Vec::new();
        execute_argv_command(&argv1, &ctx, &mut out).expect("dispatch set failed");
        assert_eq!(out, b"+OK\r\n");

        let (argv2, consumed2) = parse_argv_frame(get)
            .expect("parse get failed")
            .expect("get should be complete");
        assert_eq!(consumed2, get.len());
        out.clear();
        execute_argv_command(&argv2, &ctx, &mut out).expect("dispatch get failed");
        assert_eq!(out, b"$2\r\nv1\r\n");
    }
}
