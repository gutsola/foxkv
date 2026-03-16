use std::io;
use std::thread;

use foxkv::server::run_redis_server;
use foxkv::storage::{ConcurrentMapConfig, ConcurrentMapDb};

fn main() -> io::Result<()> {
    let cpu_count = thread::available_parallelism()
        .map(|v| v.get())
        .unwrap_or(1);
    let write_threads = 16;
    let addr = std::env::var("FOXKV_ADDR").unwrap_or_else(|_| "127.0.0.1:6380".to_string());

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(write_threads)
        .enable_all()
        .build()?;

    let db = ConcurrentMapDb::new(ConcurrentMapConfig {
        worker_count: write_threads,
    })
    .expect("failed to initialize storage");

    println!(
        "starting foxkv server on {}, cpu_count={}, write_threads={}",
        addr, cpu_count, write_threads
    );

    runtime.block_on(run_redis_server(&addr, db))
}
