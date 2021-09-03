use crate::storage::Entry;
use tokio_util::time::DelayQueue;
use tokio::sync::Mutex;
use crate::{RaftApp, RaftCore};
use std::time::Duration;
use std::sync::Arc;
use futures::StreamExt;

pub(crate) struct InsertSnapshot {
    pub e: Entry,
}
pub(crate) struct SnapshotQueue {
    q: Mutex<DelayQueue<InsertSnapshot>>,
}
impl SnapshotQueue {
    pub fn new() -> Self {
        Self {
            q: Mutex::new(DelayQueue::new()),
        }
    }
    pub async fn insert(&self, x: InsertSnapshot, delay: Duration) {
        self.q.lock().await.insert(x, delay);
    }
    pub async fn run_once<A: RaftApp>(&self, raft_core: Arc<RaftCore<A>>) {
        while let Some(Ok(expired)) = self.q.lock().await.next().await {
            let InsertSnapshot { e } = expired.into_inner();
            let snapshot_index = e.this_clock.index;
            let ok = raft_core.log.insert_snapshot(e).await.is_ok();
            if ok {
                log::info!("new snapshot entry is inserted at index {}", snapshot_index);
            }
        }
    }
}