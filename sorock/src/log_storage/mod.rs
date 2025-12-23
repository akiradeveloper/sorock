use anyhow::Result;
use crossbeam::channel::TryRecvError;
use std::sync::Arc;
use std::time::Duration;

mod reaper;
mod view;

use reaper::{LazyInsert, Reaper};
pub use view::LogShardView;

pub struct LogStorage {
    db: Arc<redb::Database>,
    tx: crossbeam::channel::Sender<LazyInsert>,
    _kill_tx: crossbeam::channel::Sender<()>,
}

impl LogStorage {
    pub fn new(db: Arc<redb::Database>) -> Self {
        let (reaper, tx) = Reaper::new(db.clone());
        let (_kill_tx, kill_rx) = crossbeam::channel::bounded(0);
        std::thread::spawn(move || loop {
            if let Err(TryRecvError::Disconnected) = kill_rx.try_recv() {
                break;
            }
            reaper.reap().ok();
        });

        Self { db, tx, _kill_tx }
    }

    pub fn get_shard(&self, shard_id: u32) -> Result<LogShardView> {
        Ok(LogShardView::new(
            self.db.clone(),
            shard_id,
            self.tx.clone(),
        )?)
    }
}

fn table_def(space: &str) -> redb::TableDefinition<'_, u64, Vec<u8>> {
    redb::TableDefinition::new(space)
}
