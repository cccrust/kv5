use crate::store::Store;
use std::sync::Arc;
use tokio::sync::broadcast;

pub struct DbDropGuard {
    db: Db,
}

pub struct Db {
    pub store: Arc<Store>,
    shutdown_complete_tx: broadcast::Sender<()>,
}

impl DbDropGuard {
    pub fn new(store: Arc<Store>, shutdown_complete_tx: broadcast::Sender<()>) -> Self {
        DbDropGuard {
            db: Db {
                store,
                shutdown_complete_tx,
            },
        }
    }

    pub fn db(&self) -> &Db {
        &self.db
    }
}

impl Drop for DbDropGuard {
    fn drop(&mut self) {
        let _ = self.db.shutdown_complete_tx.send(());
    }
}
