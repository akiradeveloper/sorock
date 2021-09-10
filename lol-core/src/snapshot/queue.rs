use crate::storage::Entry;
use crate::{RaftApp, RaftCore};
use futures::{FutureExt, StreamExt};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tokio_util::time::DelayQueue;
use tokio::sync::oneshot;

struct InsertEntry {
    e: Entry,
    tx: oneshot::Sender<bool>,
}
pub(crate) struct SnapshotQueue {
    q: Mutex<DelayQueue<InsertEntry>>,
}
impl SnapshotQueue {
    pub fn new() -> Self {
        Self {
            q: Mutex::new(DelayQueue::new()),
        }
    }
    pub async fn insert(&self, e: Entry, delay: Duration) -> impl std::future::Future<Output = bool> {
        let (tx, rx) = oneshot::channel();
        let e = InsertEntry {
            e,
            tx,
        };
        self.q.lock().await.insert(e, delay);
        rx.map(|x| {
            match x {
                Ok(true) => true,
                _ => false,
            }
        })
    }
    pub async fn run_once<A: RaftApp>(&self, raft_core: Arc<RaftCore<A>>) {
        while let Some(Ok(expired)) = self.q.lock().await.next().await {
            let InsertEntry { e, tx } = expired.into_inner();
            let snapshot_index = e.this_clock.index;
            let ok = raft_core.log.insert_snapshot(e).await.is_ok();
            let _ = tx.send(ok);
            if ok {
                log::info!("new snapshot entry is inserted at index {}", snapshot_index);
            }
        }
    }
}
