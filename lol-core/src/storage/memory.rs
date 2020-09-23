use std::collections::BTreeMap;
use std::sync::Arc;
use tokio::sync::{Mutex, RwLock};
use std::sync::atomic::{AtomicU64, Ordering};
use crate::{Entry, Vote, Index};

pub struct Storage {
    entries: Arc<RwLock<BTreeMap<u64, super::Entry>>>,
    vote: Arc<Mutex<Vote>>,
    snapshot_index: AtomicU64,
}
impl Storage {
    pub fn new() -> Self {
        Self {
            entries: Arc::new(RwLock::new(BTreeMap::new())),
            vote: Arc::new(Mutex::new(Vote::new())),
            snapshot_index: AtomicU64::new(0),
        }
    }
}
// #[async_trait::async_trait]
impl Storage {
    pub async fn get_last_index(&self) -> Index {
        let x = self.entries.read().await;
        match x.iter().next_back() {
            Some((k, _)) => *k,
            None => 0,
        }
    }
    pub async fn delete_before(&self, r: u64) {
        let ls: Vec<u64> = self.entries.read().await.range(..r).map(|x| *x.0).collect();
        for i in ls {
            self.entries.write().await.remove(&i);
        }
    }
    pub async fn insert_snapshot(&self, i: Index, e: Entry) {
        self.entries.write().await.insert(i, e);
        self.snapshot_index.fetch_max(i, Ordering::SeqCst);
    }
    pub async fn insert_entry(&self, i: Index, e: Entry) {
        self.entries.write().await.insert(i, e);
    }
    pub async fn get_entry(&self, i: Index) -> Option<Entry> {
        self.entries.read().await.get(&i).cloned()
    }
    pub async fn get_snapshot_index(&self) -> Index {
        self.snapshot_index.load(Ordering::SeqCst)
    }
    pub async fn store_vote(&self, v: Vote) {
        *self.vote.lock().await = v;
    }
    pub async fn load_vote(&self) -> Vote {
        self.vote.lock().await.clone()
    }
}