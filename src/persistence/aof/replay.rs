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

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::PathBuf;
    use std::sync::Arc;

    use super::*;
    use crate::storage::{DashMapStorageEngine, DbConfig, StorageEngine};

    fn temp_aof_path(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!("foxkv_replay_test_{}.aof", name))
    }

    fn cleanup(path: &PathBuf) {
        let _ = fs::remove_file(path);
    }

    fn test_db() -> Arc<dyn StorageEngine + Send + Sync> {
        Arc::new(DashMapStorageEngine::new(DbConfig { worker_count: 2 }).expect("db init failed"))
    }

    fn write_aof_content(path: &PathBuf, content: &[u8]) {
        fs::write(path, content).expect("failed to write aof content");
    }

    #[test]
    fn replay_commands_returns_ok_for_nonexistent_file() {
        let path = temp_aof_path("nonexistent");
        cleanup(&path);
        let db = test_db();
        let result = replay_commands(&path, db);
        assert!(result.is_ok());
        cleanup(&path);
    }

    #[test]
    fn replay_commands_returns_ok_for_empty_file() {
        let path = temp_aof_path("empty");
        cleanup(&path);
        write_aof_content(&path, b"");
        let db = test_db();
        let result = replay_commands(&path, db);
        assert!(result.is_ok());
        cleanup(&path);
    }

    #[test]
    fn replay_commands_replays_single_set_command() {
        let path = temp_aof_path("single_set");
        cleanup(&path);
        let content = b"*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n";
        write_aof_content(&path, content);
        let db = test_db();
        replay_commands(&path, db.clone()).unwrap();
        let entry = db.get_entry(b"key").expect("key should exist");
        assert_eq!(entry.value.as_ref(), b"value");
        cleanup(&path);
    }

    #[test]
    fn replay_commands_replays_multiple_commands() {
        let path = temp_aof_path("multiple_commands");
        cleanup(&path);
        let content = b"*3\r\n$3\r\nSET\r\n$2\r\nk1\r\n$2\r\nv1\r\n*3\r\n$3\r\nSET\r\n$2\r\nk2\r\n$2\r\nv2\r\n";
        write_aof_content(&path, content);
        let db = test_db();
        replay_commands(&path, db.clone()).unwrap();
        assert_eq!(db.get_entry(b"k1").unwrap().value.as_ref(), b"v1");
        assert_eq!(db.get_entry(b"k2").unwrap().value.as_ref(), b"v2");
        cleanup(&path);
    }

    #[test]
    fn replay_commands_replays_del_command() {
        let path = temp_aof_path("del_command");
        cleanup(&path);
        let content =
            b"*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n*2\r\n$3\r\nDEL\r\n$3\r\nkey\r\n";
        write_aof_content(&path, content);
        let db = test_db();
        replay_commands(&path, db.clone()).unwrap();
        assert!(db.get_entry(b"key").is_none());
        cleanup(&path);
    }

    #[test]
    fn replay_commands_replays_incr_command() {
        let path = temp_aof_path("incr_command");
        cleanup(&path);
        let content =
            b"*3\r\n$3\r\nSET\r\n$7\r\ncounter\r\n$1\r\n0\r\n*2\r\n$4\r\nINCR\r\n$7\r\ncounter\r\n";
        write_aof_content(&path, content);
        let db = test_db();
        replay_commands(&path, db.clone()).unwrap();
        let entry = db.get_entry(b"counter").expect("counter should exist");
        assert_eq!(entry.value.as_ref(), b"1");
        cleanup(&path);
    }

    #[test]
    fn replay_commands_replays_append_command() {
        let path = temp_aof_path("append_command");
        cleanup(&path);
        let content = b"*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$3\r\nabc\r\n*3\r\n$6\r\nAPPEND\r\n$3\r\nkey\r\n$3\r\ndef\r\n";
        write_aof_content(&path, content);
        let db = test_db();
        replay_commands(&path, db.clone()).unwrap();
        let entry = db.get_entry(b"key").expect("key should exist");
        assert_eq!(entry.value.as_ref(), b"abcdef");
        cleanup(&path);
    }

    #[test]
    fn replay_commands_replays_mset_command() {
        let path = temp_aof_path("mset_command");
        cleanup(&path);
        let content = b"*5\r\n$4\r\nMSET\r\n$2\r\nk1\r\n$2\r\nv1\r\n$2\r\nk2\r\n$2\r\nv2\r\n";
        write_aof_content(&path, content);
        let db = test_db();
        replay_commands(&path, db.clone()).unwrap();
        assert_eq!(db.get_entry(b"k1").unwrap().value.as_ref(), b"v1");
        assert_eq!(db.get_entry(b"k2").unwrap().value.as_ref(), b"v2");
        cleanup(&path);
    }

    #[test]
    fn replay_commands_replays_flushall_command() {
        let path = temp_aof_path("flushall_command");
        cleanup(&path);
        let set_cmd = b"*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n";
        let flushall_cmd = b"*1\r\n$8\r\nFLUSHALL\r\n";
        let mut content = Vec::new();
        content.extend_from_slice(set_cmd);
        content.extend_from_slice(flushall_cmd);
        write_aof_content(&path, &content);
        let db = test_db();
        replay_commands(&path, db.clone()).unwrap();
        assert!(db.get_entry(b"key").is_none());
        cleanup(&path);
    }

    #[test]
    fn replay_commands_returns_error_for_incomplete_frame() {
        let path = temp_aof_path("incomplete_frame");
        cleanup(&path);
        let content = b"*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nval";
        write_aof_content(&path, content);
        let db = test_db();
        let result = replay_commands(&path, db);
        assert!(result.is_err());
        cleanup(&path);
    }

    #[test]
    fn replay_commands_returns_error_for_invalid_protocol() {
        let path = temp_aof_path("invalid_protocol");
        cleanup(&path);
        let content = b"INVALID DATA";
        write_aof_content(&path, content);
        let db = test_db();
        let result = replay_commands(&path, db);
        assert!(result.is_err());
        cleanup(&path);
    }

    #[test]
    fn replay_set_commands_works_same_as_replay_commands() {
        let path = temp_aof_path("replay_set");
        cleanup(&path);
        let content = b"*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n";
        write_aof_content(&path, content);
        let db = test_db();
        replay_set_commands(&path, db.clone()).unwrap();
        let entry = db.get_entry(b"key").expect("key should exist");
        assert_eq!(entry.value.as_ref(), b"value");
        cleanup(&path);
    }

    #[test]
    fn replay_commands_replays_set_with_expire() {
        let path = temp_aof_path("set_with_expire");
        cleanup(&path);
        let content =
            b"*5\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n$2\r\nPX\r\n$10\r\n9999999999\r\n";
        write_aof_content(&path, content);
        let db = test_db();
        replay_commands(&path, db.clone()).unwrap();
        let entry = db.get_entry(b"key").expect("key should exist");
        assert_eq!(entry.value.as_ref(), b"value");
        assert!(entry.expire_at_ms.is_some());
        cleanup(&path);
    }

    #[test]
    fn replay_commands_replays_expire_command() {
        let path = temp_aof_path("expire_command");
        cleanup(&path);
        let content = b"*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n*3\r\n$6\r\nEXPIRE\r\n$3\r\nkey\r\n$10\r\n9999999999\r\n";
        write_aof_content(&path, content);
        let db = test_db();
        replay_commands(&path, db.clone()).unwrap();
        let entry = db.get_entry(b"key").expect("key should exist");
        assert!(entry.expire_at_ms.is_some());
        cleanup(&path);
    }
}
