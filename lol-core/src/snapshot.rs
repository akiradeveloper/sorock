use tokio::time::DelayQueue;
use tokio::sync::Mutex;
use std::sync::Arc;
use crate::RaftCore;
use crate::storage::Entry;
use std::time::Duration;
use futures::StreamExt;

pub struct InsertSnapshot {
    pub e: Entry,
}
pub struct SnapshotQueue {
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
    pub async fn run_once<A>(&self, raft_core: Arc<RaftCore<A>>) {
        while let Some(Ok(expired)) = self.q.lock().await.next().await {
            let x = expired.into_inner();
            raft_core.log.insert_snapshot(x.e).await;
        }
    }
}