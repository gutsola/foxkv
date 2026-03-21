use crate::app_context::AppContext;
use crate::command::{ExecOutcome, ExecTransition};
use crate::command::shared::args::required_arg;
use crate::replication::PsyncDecision;
use crate::resp::append_simple_response;

macro_rules! replication_commands {
    ($m:ident) => {
        $m!(replication, replconf, cmd_replconf);
    };
}
pub(crate) use replication_commands;

pub fn cmd_replconf(args: &[&[u8]], ctx: &AppContext, out: &mut Vec<u8>) -> Result<(), String> {
    if args.is_empty() {
        return Err("ERR syntax error".to_string());
    }
    if args.len() >= 2 && args[0].eq_ignore_ascii_case(b"ACK") {
        let ack_offset = std::str::from_utf8(args[1])
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .ok_or_else(|| "ERR invalid replication offset".to_string())?;
        ctx.replication.record_ack(ack_offset);
    }
    // Keep behavior lenient for other REPLCONF subcommands.
    append_simple_response(out, "OK");
    Ok(())
}

pub fn cmd_psync(
    args: &[&[u8]],
    ctx: &AppContext,
    out: &mut Vec<u8>,
) -> Result<ExecOutcome, String> {
    let replid_raw = required_arg(args, 0)?;
    let offset_raw = required_arg(args, 1)?;
    let replid = std::str::from_utf8(replid_raw).map_err(|_| "ERR invalid replid".to_string())?;
    let offset = std::str::from_utf8(offset_raw)
        .ok()
        .and_then(|v| v.parse::<i64>().ok())
        .ok_or_else(|| "ERR invalid replication offset".to_string())?;

    match ctx.replication.negotiate_psync(replid, offset) {
        PsyncDecision::Continue { start_offset } => {
            append_simple_response(out, "CONTINUE");
            Ok(ExecOutcome {
                transition: ExecTransition::EnterReplicaStream {
                    start_offset,
                    send_empty_rdb: false,
                },
            })
        }
        PsyncDecision::FullResync {
            replid,
            current_offset,
        } => {
            append_simple_response(out, &format!("FULLRESYNC {replid} {current_offset}"));
            Ok(ExecOutcome {
                transition: ExecTransition::EnterReplicaStream {
                    start_offset: current_offset.saturating_add(1),
                    send_empty_rdb: true,
                },
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::app_context::AppContext;
    use crate::command::ExecTransition;
    use crate::config::default_config;
    use crate::replication::ReplicationManager;
    use crate::storage::{DashMapStorageEngine, DbConfig, StorageEngine};

    use super::{cmd_psync, cmd_replconf};

    fn test_ctx() -> AppContext {
        let db: Arc<dyn StorageEngine + Send + Sync> = Arc::new(
            DashMapStorageEngine::new(DbConfig { worker_count: 2 }).expect("db init should work"),
        );
        AppContext::new(
            default_config(),
            db,
            None,
            None,
            None,
            Arc::new(ReplicationManager::new()),
        )
    }

    #[test]
    fn replconf_requires_arguments_and_valid_ack_offset() {
        let ctx = test_ctx();
        let mut out = Vec::new();

        let err = cmd_replconf(&[], &ctx, &mut out).expect_err("missing args");
        assert_eq!(err, "ERR syntax error");

        let err = cmd_replconf(&[b"ACK", b"not-number"], &ctx, &mut out).expect_err("invalid ack");
        assert_eq!(err, "ERR invalid replication offset");
    }

    #[test]
    fn replconf_ack_updates_metrics_and_returns_ok() {
        let ctx = test_ctx();
        let mut out = Vec::new();

        cmd_replconf(&[b"ACK", b"123"], &ctx, &mut out).expect("ack should succeed");
        assert_eq!(out, b"+OK\r\n");
        let metrics = ctx.replication.replication_metrics();
        assert_eq!(metrics.last_ack_offset, 123);
        assert_eq!(metrics.ack_count, 1);
    }

    #[test]
    fn replconf_is_lenient_for_other_subcommands() {
        let ctx = test_ctx();
        let mut out = Vec::new();
        cmd_replconf(&[b"listening-port", b"6380"], &ctx, &mut out).expect("lenient mode");
        assert_eq!(out, b"+OK\r\n");
    }

    #[test]
    fn psync_validates_replid_and_offset_arguments() {
        let ctx = test_ctx();
        let mut out = Vec::new();

        let err = cmd_psync(&[b"\xff", b"0"], &ctx, &mut out).expect_err("invalid replid utf8");
        assert_eq!(err, "ERR invalid replid");

        let err = cmd_psync(&[b"?", b"abc"], &ctx, &mut out).expect_err("invalid offset");
        assert_eq!(err, "ERR invalid replication offset");
    }

    #[test]
    fn psync_returns_fullresync_for_unknown_replid() {
        let ctx = test_ctx();
        let mut out = Vec::new();
        let result = cmd_psync(&[b"?", b"-1"], &ctx, &mut out).expect("psync should succeed");
        let output = String::from_utf8(out).expect("utf8");
        assert!(output.starts_with("+FULLRESYNC "));
        assert!(matches!(
            result.transition,
            ExecTransition::EnterReplicaStream {
                send_empty_rdb: true,
                ..
            }
        ));
    }

    #[test]
    fn psync_returns_continue_when_replid_matches_current_offset() {
        let ctx = test_ctx();
        let replid = ctx.replication.replid().to_string();
        let mut out = Vec::new();

        let result = cmd_psync(&[replid.as_bytes(), b"0"], &ctx, &mut out).expect("psync continue");
        assert_eq!(out, b"+CONTINUE\r\n");
        assert!(matches!(
            result.transition,
            ExecTransition::EnterReplicaStream {
                start_offset: 1,
                send_empty_rdb: false
            }
        ));
    }
}
