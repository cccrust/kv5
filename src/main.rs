use kv5::db::DbDropGuard;
use kv5::server::Listener;
use kv5::store::Store;

use anyhow::Result;
use std::sync::Arc;
use tokio::sync::broadcast;

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

    let (notify_shutdown, shutdown_complete_rx) = broadcast::channel(1);
    let db = Arc::new(DbDropGuard::new(store.clone(), notify_shutdown.clone()));

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

    let mut server = Listener::new(&addr, db, notify_shutdown, shutdown_complete_rx).await?;
    server.run().await?;

    Ok(())
}
