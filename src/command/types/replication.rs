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
