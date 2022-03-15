use crate::storage::Entry;
use crate::RaftCore;
use futures::{FutureExt, StreamExt};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::oneshot;
use tokio::sync::Mutex;
use tokio_util::time::DelayQueue;

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
    pub async fn insert(
        &self,
        e: Entry,
        delay: Duration,
    ) -> impl std::future::Future<Output = bool> {
        let (tx, rx) = oneshot::channel();
        let e = InsertEntry { e, tx };
        self.q.lock().await.insert(e, delay);
        rx.map(|x| match x {
            Ok(true) => true,
            _ => false,
        })
    }
    pub async fn run_once(&self, raft_core: Arc<RaftCore>) {
        loop {
            let mut writer = self.q.lock().await;
            let fut = futures::future::poll_fn(|ctx| writer.poll_expired(ctx));
            match futures::future::poll_immediate(fut).await {
                // First `Some` means it is ready.
                // Second `Some` means there is an entry in the queue.
                Some(Some(expired)) => {
                    let InsertEntry { e, tx } = expired.into_inner();
                    let snapshot_index = e.this_clock.index;
                    let ok = raft_core.log.insert_snapshot(e).await.is_ok();
                    let _ = tx.send(ok);
                    if ok {
                        log::info!("new snapshot entry is inserted at index {}", snapshot_index);
                    }
                }
                // If it is pending or queue is empty.
                // Don't do anything but break.
                _ => break,
            }
        }
    }
}
