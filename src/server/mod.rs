use std::io;
use std::sync::Arc;
use std::time::Duration;

use log::debug;
use tokio::net::TcpListener;

use crate::app_context::AppContext;
use crate::persistence::rdb::{maybe_trigger_bgsave, RdbRuntimeConfig};

mod connection;

pub async fn run_server(addr: &str, ctx: Arc<AppContext>) -> io::Result<()> {
    ctx.replication.start_ingress_worker();
    spawn_rdb_save_rules_task(&ctx);

    let listener = TcpListener::bind(addr).await?;
    loop {
        let (stream, peer_addr) = listener.accept().await?;
        debug!("# New client connected from {}", peer_addr);
        if let Err(err) = stream.set_nodelay(true) {
            eprintln!("set_nodelay failed: {err}");
        }
        let ctx = Arc::clone(&ctx);
        tokio::spawn(async move {
            match connection::handle_connection(stream, peer_addr, ctx).await {
                Ok(()) => {
                    debug!("# Client disconnected: {}", peer_addr);
                }
                Err(err) => {
                    debug!("# Connection error from {}: {}", peer_addr, err);
                }
            }
        });
    }
}

fn spawn_rdb_save_rules_task(ctx: &Arc<AppContext>) {
    let Some(tracker) = ctx.rdb_dirty_tracker.clone() else {
        return;
    };
    let Some(bgsave_in_progress) = ctx.rdb_bgsave_in_progress.clone() else {
        return;
    };

    let db = ctx.db.clone();
    let rdb_config = RdbRuntimeConfig::from_config(&ctx.config.rdb);
    let hz = ctx.config.hz.max(1) as u64;
    let interval_ms = (1000 / hz).max(1);

    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_millis(interval_ms));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            interval.tick().await;
            maybe_trigger_bgsave(
                db.clone(),
                rdb_config.clone(),
                tracker.clone(),
                bgsave_in_progress.clone(),
            );
        }
    });
}

