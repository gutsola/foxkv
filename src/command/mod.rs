use smallvec::SmallVec;

mod shared;
pub mod types;

use crate::app_context::AppContext;

#[derive(Clone, Copy)]
pub enum SetCondition {
    None,
    Nx,
    Xx,
}

pub type Argv<'a> = SmallVec<[&'a [u8]; 8]>;

pub fn parse_argv_frame(input: &[u8]) -> Option<(Argv<'_>, usize)> {
    if input.is_empty() {
        return None;
    }
    if input[0] != b'*' {
        return None;
    }
    let (arg_count, mut cursor) = parse_usize_line(input, 1)?;
    let mut argv = Argv::with_capacity(arg_count);
    for _ in 0..arg_count {
        if cursor >= input.len() || input[cursor] != b'$' {
            return None;
        }
        cursor += 1;

        let (bulk_len, next_cursor) = parse_usize_line(input, cursor)?;
        let data_start = next_cursor;
        let data_end = data_start.checked_add(bulk_len)?;
        let frame_end = data_end.checked_add(2)?;
        if frame_end > input.len() {
            return None;
        }

        argv.push(&input[data_start..data_end]);
        cursor = frame_end;
    }
    Some((argv, cursor))
}

pub fn execute_argv_command(
    argv: &[&[u8]],
    ctx: &AppContext,
    out: &mut Vec<u8>,
) -> Result<(), String> {
    let Some((cmd, args)) = argv.split_first() else {
        return Err("ERR Protocol error: empty command".to_string());
    };

    macro_rules! emit_dispatch {
        ($module:ident, $name:ident, $handler:ident) => {
            if cmd.eq_ignore_ascii_case(stringify!($name).as_bytes()) {
                return types::$module::$handler(args, ctx, out);
            }
        };
    }

    types::string::string_commands!(emit_dispatch);
    types::hash::hash_commands!(emit_dispatch);
    types::list::list_commands!(emit_dispatch);
    types::set::set_commands!(emit_dispatch);
    types::zset::zset_commands!(emit_dispatch);

    let name = String::from_utf8_lossy(cmd);
    Err(format!("ERR unknown command '{}'", name))
}

fn parse_usize_line(input: &[u8], mut cursor: usize) -> Option<(usize, usize)> {
    let mut value = 0_usize;
    loop {
        if cursor >= input.len() {
            return None;
        }
        let byte = input[cursor];
        if byte == b'\r' {
            if cursor + 1 >= input.len() || input[cursor + 1] != b'\n' {
                return None;
            }
            return Some((value, cursor + 2));
        }
        value = value
            .checked_mul(10)?
            .checked_add(usize::from(byte.wrapping_sub(b'0')))?;
        cursor += 1;
    }
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

        let (argv1, consumed1) = parse_argv_frame(set).expect("set should be complete");
        assert_eq!(consumed1, set.len());
        let mut out = Vec::new();
        execute_argv_command(&argv1, &ctx, &mut out).expect("dispatch set failed");
        assert_eq!(out, b"+OK\r\n");

        let (argv2, consumed2) = parse_argv_frame(get).expect("get should be complete");
        assert_eq!(consumed2, get.len());
        out.clear();
        execute_argv_command(&argv2, &ctx, &mut out).expect("dispatch get failed");
        assert_eq!(out, b"$2\r\nv1\r\n");
    }

    #[test]
    fn zadd_dispatch_works() {
        let db: Arc<dyn StorageEngine + Send + Sync> = Arc::new(
            DashMapStorageEngine::new(DbConfig { worker_count: 2 }).expect("db init failed"),
        );
        let ctx = AppContext::new(default_config(), db, None);
        // ZADD key 1 a 2 b (6 args: ZADD, key, score1, member1, score2, member2)
        let zadd = b"*6\r\n$4\r\nZADD\r\n$3\r\nkey\r\n$1\r\n1\r\n$1\r\na\r\n$1\r\n2\r\n$1\r\nb\r\n";

        let (argv, _consumed) = parse_argv_frame(zadd).expect("zadd should be complete");
        assert_eq!(argv.len(), 6);
        let mut out = Vec::new();
        execute_argv_command(&argv, &ctx, &mut out).expect("dispatch zadd failed");
        assert_eq!(out, b":2\r\n");
    }
}
