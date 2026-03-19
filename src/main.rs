use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::thread;

use foxkv::app_context::AppContext;
use foxkv::config::{self, AppConfig};
use foxkv::persistence::aof::{AofEngine, AofRuntimeConfig, replay_commands};
use foxkv::server::run_redis_server;
use foxkv::storage::{DashMapStorageEngine, DbConfig, StorageEngine};

fn main() -> io::Result<()> {
    let config = load_config_from_args()
        .map_err(|err| io::Error::other(format!("failed to load config: {err}")))?;
    let cpu_count = thread::available_parallelism()
        .map(|v| v.get())
        .unwrap_or(1);
    let write_threads = 16;
    let addr = config.listen_addr();

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(write_threads)
        .enable_all()
        .build()?;

    let db: Arc<dyn StorageEngine + Send + Sync> = Arc::new(
        DashMapStorageEngine::new(DbConfig {
            worker_count: write_threads,
        })
        .expect("failed to initialize storage"),
    );
    let aof = initialize_aof(&config, db.clone())
        .map_err(|err| io::Error::other(format!("failed to initialize aof: {err}")))?;
    let ctx = Arc::new(AppContext::new(config.clone(), db, aof));

    println!(
        "starting foxkv server on {}, cpu_count={}, write_threads={}, bind={:?}, port={}, aof_enabled={}, rdb_dir={}, rdb_file={}",
        addr,
        cpu_count,
        write_threads,
        config.bind,
        config.port,
        config.aof.enabled,
        config.rdb.dir.display(),
        config.rdb.dbfilename
    );

    runtime.block_on(run_redis_server(&addr, ctx))
}

fn load_config_from_args() -> Result<AppConfig, config::ConfigError> {
    let mut args = std::env::args().skip(1);
    let mut config_path: Option<PathBuf> = None;
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--config" => {
                let value = args.next().ok_or_else(|| {
                    config::ConfigError::Parse("--config requires a file path".to_string())
                })?;
                config_path = Some(PathBuf::from(value));
            }
            "-h" | "--help" => {
                println!("Usage: foxkv [--config <path>]");
                std::process::exit(0);
            }
            _ => {
                return Err(config::ConfigError::Parse(format!(
                    "unknown argument '{}'",
                    arg
                )));
            }
        }
    }

    if let Some(path) = config_path {
        return config::load_from_path(&path);
    }

    let default_path = Path::new("redis.conf");
    if default_path.exists() {
        return config::load_from_path(default_path);
    }

    Ok(config::default_config())
}

fn initialize_aof(
    config: &AppConfig,
    db: Arc<dyn StorageEngine + Send + Sync>,
) -> io::Result<Option<AofEngine>> {
    let runtime_cfg = AofRuntimeConfig::from_config(&config.rdb.dir, &config.aof);
    if !runtime_cfg.enabled {
        return Ok(None);
    }
    replay_commands(&runtime_cfg.file_path, db)?;
    let engine = AofEngine::open(runtime_cfg)?;
    Ok(Some(engine))
}
