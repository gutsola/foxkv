use std::io;
use std::path::Path;
use std::sync::Arc;

use crate::app_context::AppContext;
use crate::command::{execute_argv_command, parse_argv_frame};
use crate::config::default_config;
use crate::replication::ReplicationManager;
use crate::storage::StorageEngine;

pub fn replay_commands(path: &Path, db: Arc<dyn StorageEngine + Send + Sync>) -> io::Result<()> {
    if !path.exists() {
        return Ok(());
    }
    let bytes = std::fs::read(path)?;
    let mut cursor = 0_usize;
    let replay_ctx = AppContext::new(
        default_config(),
        db,
        None,
        None,
        None,
        Arc::new(ReplicationManager::new()),
    );
    let mut response_sink = Vec::new();

    while cursor < bytes.len() {
        let (argv, consumed) = parse_argv_frame(&bytes[cursor..]).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "incomplete command at end of AOF file",
            )
        })?;
        response_sink.clear();
        execute_argv_command(&argv, &replay_ctx, &mut response_sink)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        cursor += consumed;
    }
    Ok(())
}

pub fn replay_set_commands(
    path: &Path,
    db: Arc<dyn StorageEngine + Send + Sync>,
) -> io::Result<()> {
    replay_commands(path, db)
}
