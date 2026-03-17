use std::io;
use std::sync::Arc;

use tokio::net::TcpListener;

use crate::app_context::AppContext;

mod connection;

pub async fn run_redis_server(addr: &str, ctx: Arc<AppContext>) -> io::Result<()> {
    let listener = TcpListener::bind(addr).await?;
    loop {
        let (stream, _) = listener.accept().await?;
        if let Err(err) = stream.set_nodelay(true) {
            eprintln!("set_nodelay failed: {err}");
        }
        let ctx = Arc::clone(&ctx);
        tokio::spawn(async move {
            if let Err(err) = connection::handle_connection(stream, ctx).await {
                eprintln!("connection error: {err}");
            }
        });
    }
}
