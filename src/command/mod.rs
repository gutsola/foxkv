use smallvec::SmallVec;

pub mod shared;
pub mod types;

use crate::app_context::AppContext;

#[derive(Clone, Copy)]
pub enum SetCondition {
    None,
    Nx,
    Xx,
}

pub type Argv<'a> = SmallVec<[&'a [u8]; 8]>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExecTransition {
    None,
    EnterReplicaStream {
        start_offset: u64,
        send_empty_rdb: bool,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ExecOutcome {
    pub transition: ExecTransition,
}

impl Default for ExecOutcome {
    fn default() -> Self {
        Self {
            transition: ExecTransition::None,
        }
    }
}

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
) -> Result<ExecOutcome, String> {
    execute_argv_command_inner(argv, ctx, out, true)
}

pub fn execute_replication_argv_command(
    argv: &[&[u8]],
    ctx: &AppContext,
    out: &mut Vec<u8>,
) -> Result<ExecOutcome, String> {
    execute_argv_command_inner(argv, ctx, out, false)
}

fn execute_argv_command_inner(
    argv: &[&[u8]],
    ctx: &AppContext,
    out: &mut Vec<u8>,
    replicate_writes: bool,
) -> Result<ExecOutcome, String> {
    let Some((cmd, args)) = argv.split_first() else {
        return Err("ERR Protocol error: empty command".to_string());
    };

    if cmd.eq_ignore_ascii_case(b"PSYNC") {
        return types::replication::cmd_psync(args, ctx, out);
    }
    if cmd.eq_ignore_ascii_case(b"SYNC") {
        return types::replication::cmd_psync(&[b"?", b"-1"], ctx, out);
    }

    macro_rules! emit_dispatch {
        ($module:ident, $name:ident, $handler:ident) => {
            if cmd.eq_ignore_ascii_case(stringify!($name).as_bytes()) {
                types::$module::$handler(args, ctx, out)?;
                if replicate_writes
                    && is_replicable_write_command(cmd)
                    && ctx.replication.should_capture_writes()
                {
                    ctx.replication.try_enqueue_write_argv(argv);
                }
                return Ok(ExecOutcome::default());
            }
        };
    }
    
    types::string::string_commands!(emit_dispatch);
    types::hash::hash_commands!(emit_dispatch);
    types::list::list_commands!(emit_dispatch);
    types::set::set_commands!(emit_dispatch);
    types::zset::zset_commands!(emit_dispatch);
    types::connection::connection_commands!(emit_dispatch);
    types::server::server_commands!(emit_dispatch);
    types::generic::generic_commands!(emit_dispatch);
    types::replication::replication_commands!(emit_dispatch);

    let name = String::from_utf8_lossy(cmd);
    Err(format!("ERR unknown command '{}'", name))
}

fn is_replicable_write_command(cmd: &[u8]) -> bool {
    cmd.eq_ignore_ascii_case(b"APPEND")
        || cmd.eq_ignore_ascii_case(b"DECR")
        || cmd.eq_ignore_ascii_case(b"DECRBY")
        || cmd.eq_ignore_ascii_case(b"GETSET")
        || cmd.eq_ignore_ascii_case(b"INCR")
        || cmd.eq_ignore_ascii_case(b"INCRBY")
        || cmd.eq_ignore_ascii_case(b"INCRBYFLOAT")
        || cmd.eq_ignore_ascii_case(b"MSET")
        || cmd.eq_ignore_ascii_case(b"MSETNX")
        || cmd.eq_ignore_ascii_case(b"PSETEX")
        || cmd.eq_ignore_ascii_case(b"SET")
        || cmd.eq_ignore_ascii_case(b"SETEX")
        || cmd.eq_ignore_ascii_case(b"SETNX")
        || cmd.eq_ignore_ascii_case(b"SETRANGE")
        || cmd.eq_ignore_ascii_case(b"HDEL")
        || cmd.eq_ignore_ascii_case(b"HINCRBY")
        || cmd.eq_ignore_ascii_case(b"HINCRBYFLOAT")
        || cmd.eq_ignore_ascii_case(b"HMSET")
        || cmd.eq_ignore_ascii_case(b"HSET")
        || cmd.eq_ignore_ascii_case(b"HSETNX")
        || cmd.eq_ignore_ascii_case(b"LPUSH")
        || cmd.eq_ignore_ascii_case(b"LPUSHX")
        || cmd.eq_ignore_ascii_case(b"RPUSH")
        || cmd.eq_ignore_ascii_case(b"RPUSHX")
        || cmd.eq_ignore_ascii_case(b"LPOP")
        || cmd.eq_ignore_ascii_case(b"RPOP")
        || cmd.eq_ignore_ascii_case(b"BRPOPLPUSH")
        || cmd.eq_ignore_ascii_case(b"RPOPLPUSH")
        || cmd.eq_ignore_ascii_case(b"LINSERT")
        || cmd.eq_ignore_ascii_case(b"LREM")
        || cmd.eq_ignore_ascii_case(b"LSET")
        || cmd.eq_ignore_ascii_case(b"LTRIM")
        || cmd.eq_ignore_ascii_case(b"SADD")
        || cmd.eq_ignore_ascii_case(b"SDIFFSTORE")
        || cmd.eq_ignore_ascii_case(b"SINTERSTORE")
        || cmd.eq_ignore_ascii_case(b"SMOVE")
        || cmd.eq_ignore_ascii_case(b"SPOP")
        || cmd.eq_ignore_ascii_case(b"SREM")
        || cmd.eq_ignore_ascii_case(b"SUNIONSTORE")
        || cmd.eq_ignore_ascii_case(b"ZADD")
        || cmd.eq_ignore_ascii_case(b"ZINCRBY")
        || cmd.eq_ignore_ascii_case(b"ZINTERSTORE")
        || cmd.eq_ignore_ascii_case(b"ZPOPMAX")
        || cmd.eq_ignore_ascii_case(b"ZPOPMIN")
        || cmd.eq_ignore_ascii_case(b"ZREM")
        || cmd.eq_ignore_ascii_case(b"ZREMRANGEBYLEX")
        || cmd.eq_ignore_ascii_case(b"ZREMRANGEBYRANK")
        || cmd.eq_ignore_ascii_case(b"ZREMRANGEBYSCORE")
        || cmd.eq_ignore_ascii_case(b"ZUNIONSTORE")
        || cmd.eq_ignore_ascii_case(b"DEL")
        || cmd.eq_ignore_ascii_case(b"EXPIRE")
        || cmd.eq_ignore_ascii_case(b"EXPIREAT")
        || cmd.eq_ignore_ascii_case(b"PERSIST")
        || cmd.eq_ignore_ascii_case(b"PEXPIRE")
        || cmd.eq_ignore_ascii_case(b"PEXPIREAT")
        || cmd.eq_ignore_ascii_case(b"FLUSHALL")
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
    use crate::replication::ReplicationManager;
    use crate::storage::{DashMapStorageEngine, DbConfig, StorageEngine};

    #[test]
    fn string_set_then_get_dispatch_works() {
        let db: Arc<dyn StorageEngine + Send + Sync> = Arc::new(
            DashMapStorageEngine::new(DbConfig { worker_count: 2 }).expect("db init failed"),
        );
        let ctx = AppContext::new(
            default_config(),
            db,
            None,
            None,
            None,
            Arc::new(ReplicationManager::new()),
        );
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
        let ctx = AppContext::new(
            default_config(),
            db,
            None,
            None,
            None,
            Arc::new(ReplicationManager::new()),
        );
        // ZADD key 1 a 2 b (6 args: ZADD, key, score1, member1, score2, member2)
        let zadd = b"*6\r\n$4\r\nZADD\r\n$3\r\nkey\r\n$1\r\n1\r\n$1\r\na\r\n$1\r\n2\r\n$1\r\nb\r\n";

        let (argv, _consumed) = parse_argv_frame(zadd).expect("zadd should be complete");
        assert_eq!(argv.len(), 6);
        let mut out = Vec::new();
        execute_argv_command(&argv, &ctx, &mut out).expect("dispatch zadd failed");
        assert_eq!(out, b":2\r\n");
    }
}
