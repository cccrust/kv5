use crate::cmd;
use crate::connection::Connection;
use crate::db::DbDropGuard;
use crate::shutdown::Shutdown;
use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};
use tokio::signal;
use tokio::sync::broadcast;

const MAX_CONNECTIONS: usize = 250;

#[allow(dead_code)]
pub struct Listener {
    listener: TcpListener,
    notify_shutdown: broadcast::Sender<()>,
    shutdown_complete_rx: broadcast::Receiver<()>,
    db: Arc<DbDropGuard>,
    limit_connections: Arc<tokio::sync::Semaphore>,
}

impl Listener {
    pub async fn new(
        addr: &str,
        db: Arc<DbDropGuard>,
        notify_shutdown: broadcast::Sender<()>,
        shutdown_complete_rx: broadcast::Receiver<()>,
    ) -> anyhow::Result<Self> {
        let listener = TcpListener::bind(addr).await?;
        let limit_connections = Arc::new(tokio::sync::Semaphore::new(MAX_CONNECTIONS));

        tracing::info!("kv5 server listening on {}", addr);
        tracing::info!("Compatible with redis-cli: redis-cli -p 6380");

        Ok(Listener {
            listener,
            notify_shutdown,
            shutdown_complete_rx,
            db,
            limit_connections,
        })
    }

    pub async fn run(&mut self) -> anyhow::Result<()> {
        tokio::spawn(async move {
            let _ = signal::ctrl_c().await;
            tracing::info!("Shutting down, saving data...");
        });

        loop {
            let permit = self
                .limit_connections
                .clone()
                .acquire_owned()
                .await
                .unwrap();

            match self.listener.accept().await {
                Ok((socket, peer)) => {
                    tracing::debug!("New connection from {}", peer);
                    let db = self.db.clone();
                    let notify_shutdown = self.notify_shutdown.clone();

                    tokio::spawn(async move {
                        drop(permit);
                        if let Err(e) = handle_connection(socket, db, notify_shutdown).await {
                            tracing::debug!("Connection {} closed: {}", peer, e);
                        }
                    });
                }
                Err(e) => {
                    tracing::warn!("Accept error: {}", e);
                }
            }
        }
    }
}

async fn handle_connection(
    socket: TcpStream,
    db: Arc<DbDropGuard>,
    notify_shutdown: broadcast::Sender<()>,
) -> anyhow::Result<()> {
    let mut connection = Connection::new(socket);
    let mut shutdown = Shutdown::new(notify_shutdown.subscribe());

    loop {
        let maybe_frame = tokio::select! {
            result = connection.read_frame() => result?,
            _ = shutdown.recv() => {
                let _ = db.db().store.save_to_disk();
                return Ok(());
            }
        };

        match maybe_frame {
            Some(frame) => {
                let response = cmd::handle_command(db.db().store.clone(), frame);
                connection.write_frame(&response).await?;
                connection.flush().await?;
            }
            None => {
                return Ok(());
            }
        }
    }
}
