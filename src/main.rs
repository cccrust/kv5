use kv5::cmd;
use kv5::resp::RespParser;
use kv5::store::Store;

use anyhow::Result;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::signal;

const DEFAULT_ADDR: &str = "127.0.0.1:6380";
const DEFAULT_PERSIST: &str = "kv5.dump.json";

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(std::env::var("RUST_LOG").unwrap_or_else(|_| "kv5=info".to_string()))
        .init();

    let addr = std::env::var("KV5_ADDR").unwrap_or_else(|_| DEFAULT_ADDR.to_string());
    let persist = std::env::var("KV5_PERSIST")
        .ok()
        .or_else(|| Some(DEFAULT_PERSIST.to_string()));

    let store = Arc::new(Store::new(persist));
    Store::start_expiry_task(store.clone());

    let listener = TcpListener::bind(&addr).await?;
    tracing::info!("kv5 server listening on {}", addr);
    tracing::info!("Compatible with redis-cli: redis-cli -p 6380");

    {
        let store_snap = store.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(60));
            interval.tick().await;
            loop {
                interval.tick().await;
                if let Err(e) = store_snap.save_to_disk() {
                    tracing::warn!("Auto-save failed: {}", e);
                }
            }
        });
    }

    let store_shutdown = store.clone();
    tokio::spawn(async move {
        signal::ctrl_c().await.expect("failed to listen for Ctrl+C");
        tracing::info!("Shutting down, saving data...");
        let _ = store_shutdown.save_to_disk();
        std::process::exit(0);
    });

    loop {
        let (socket, peer) = listener.accept().await?;
        tracing::debug!("New connection from {}", peer);
        let store = store.clone();
        tokio::spawn(async move {
            if let Err(e) = handle_connection(socket, store).await {
                tracing::debug!("Connection {} closed: {}", peer, e);
            }
        });
    }
}

async fn handle_connection(mut socket: TcpStream, store: Arc<Store>) -> Result<()> {
    let mut buf = vec![0u8; 4096];
    let mut pending = Vec::<u8>::new();

    loop {
        let n = socket.read(&mut buf).await?;
        if n == 0 {
            return Ok(());
        }

        pending.extend_from_slice(&buf[..n]);

        loop {
            let mut parser = RespParser::new(pending.clone());
            match parser.parse() {
                Ok(Some(value)) => {
                    let consumed = parser.consumed();
                    pending.drain(..consumed);
                    let response = cmd::handle_command(store.clone(), value);
                    socket.write_all(&response.serialize()).await?;
                }
                Ok(None) => break,
                Err(_) => break,
            }
        }
    }
}
