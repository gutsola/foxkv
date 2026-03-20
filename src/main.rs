use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use foxkv::app_context::AppContext;
use foxkv::config::{self, AppConfig};
use foxkv::persistence::aof::{AofEngine, AofRuntimeConfig, replay_commands};
use foxkv::persistence::rdb::RdbDirtyTracker;
use foxkv::persistence::rdb_dirty_wrapper::StorageWithRdbDirty;
use foxkv::server::run_server;
use foxkv::storage::{DashMapStorageEngine, DbConfig, StorageEngine};

const VERSION: &str = env!("CARGO_PKG_VERSION");

fn main() -> io::Result<()> {
    let config = load_config_from_args()
        .map_err(|err| io::Error::other(format!("failed to load config: {err}")))?;
    let write_threads = 16;
    let addr = config.listen_addr();
    let pid = std::process::id();

    eprintln!("# oO0OoO0OoO0Oo Foxkv is starting oO0OoO0OoO0Oo");
    eprintln!(
        "# Foxkv version={}, bits=64, pid={}, just started",
        VERSION, pid
    );
    eprintln!("# Configuration loaded");

    print_startup_logo(config.port, pid);

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(write_threads)
        .enable_all()
        .build()?;

    let raw_db: Arc<dyn StorageEngine + Send + Sync> = Arc::new(
        DashMapStorageEngine::new(DbConfig {
            worker_count: write_threads,
        })
        .expect("failed to initialize storage"),
    );

    eprintln!("# Server initialized");

    let load_start = Instant::now();
    let aof = initialize_aof(&config, raw_db.clone())
        .map_err(|err| io::Error::other(format!("failed to initialize aof: {err}")))?;
    let load_elapsed = load_start.elapsed();
    let load_secs = load_elapsed.as_secs_f64();

    if config.aof.enabled {
        eprintln!(
            "# DB loaded from append only file: {:.3} seconds",
            load_secs
        );
    } else {
        eprintln!("# DB loaded from disk: {:.3} seconds", load_secs);
    }

    let (db, rdb_dirty_tracker, rdb_bgsave_in_progress): (
        Arc<dyn StorageEngine + Send + Sync>,
        Option<Arc<RdbDirtyTracker>>,
        Option<Arc<AtomicBool>>,
    ) = if config.rdb.save_rules.is_empty() {
        eprintln!("# RDB disabled (no save rules)");
        (raw_db, None, None)
    } else {
        let tracker = Arc::new(RdbDirtyTracker::new());
        let now_secs = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        tracker.set_last_save(now_secs);
        let bgsave_in_progress = Arc::new(AtomicBool::new(false));
        let wrapped = Arc::new(StorageWithRdbDirty::new(raw_db, tracker.clone()));
        (wrapped, Some(tracker), Some(bgsave_in_progress))
    };

    eprintln!("# Ready to accept connections");

    let ctx = Arc::new(AppContext::new(
        config.clone(),
        db,
        aof,
        rdb_dirty_tracker,
        rdb_bgsave_in_progress,
    ));
    runtime.block_on(async {
        let server = run_server(&addr, ctx.clone());
        tokio::select! {
            _ = server => {}
            _ = wait_for_shutdown_signal() => {
                if let Some(ref aof) = ctx.aof {
                    if let Err(e) = aof.sync_data() {
                        eprintln!("# AOF sync failed: {e}");
                    }
                }
                eprintln!("Bye Bye!");
            }
        }
    });
    Ok(())
}

#[cfg(unix)]
async fn wait_for_shutdown_signal() {
    use tokio::signal::unix::{signal, SignalKind};
    let mut sigterm = signal(SignalKind::terminate()).expect("failed to register SIGTERM handler");
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {}
        _ = sigterm.recv() => {}
    }
}

#[cfg(not(unix))]
async fn wait_for_shutdown_signal() {
    tokio::signal::ctrl_c().await.expect("failed to listen for Ctrl+C");
}

fn print_startup_logo(port: u16, pid: u32) {
    let logo = r#"          /\   /\
         /  \_/  \            Foxkv {version} 64 bit
        |  o   o  |           Running in standalone mode
         \   w   /            Port: {port}
          \_____/             PID: {pid}
         /       \
    ____/  ~~~   \____
"#;
    eprint!(
        "{}",
        logo.replace("{version}", VERSION)
            .replace("{port}", &port.to_string())
            .replace("{pid}", &pid.to_string())
    );
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
