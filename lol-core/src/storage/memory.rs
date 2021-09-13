use super::{Ballot, Entry};
use crate::Index;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::{Mutex, RwLock};

pub struct Storage {
    entries: Arc<RwLock<BTreeMap<u64, super::Entry>>>,
    ballot: Arc<Mutex<Ballot>>,
    snapshot_index: AtomicU64,
    tags: Arc<RwLock<BTreeMap<u64, crate::SnapshotTag>>>,
}
impl Storage {
    pub fn new() -> Self {
        Self {
            entries: Arc::new(RwLock::new(BTreeMap::new())),
            ballot: Arc::new(Mutex::new(Ballot::new())),
            snapshot_index: AtomicU64::new(0),
            tags: Arc::new(RwLock::new(BTreeMap::new())),
        }
    }
}
use anyhow::Result;
#[async_trait::async_trait]
impl super::RaftStorage for Storage {
    async fn delete_tag(&self, i: Index) -> Result<()> {
        self.tags.write().await.remove(&i);
        Ok(())
    }
    async fn list_tags(&self) -> Result<BTreeSet<Index>> {
        let mut r = BTreeSet::new();
        for k in self.tags.read().await.keys() {
            r.insert(*k);
        }
        Ok(r)
    }
    async fn get_tag(&self, i: Index) -> Result<Option<crate::SnapshotTag>> {
        let r = self.tags.read().await.get(&i).cloned();
        Ok(r)
    }
    async fn put_tag(&self, i: Index, x: crate::SnapshotTag) -> Result<()> {
        self.tags.write().await.insert(i, x);
        Ok(())
    }
    async fn get_last_index(&self) -> Result<Index> {
        let x = self.entries.read().await;
        let r = match x.iter().next_back() {
            Some((k, _)) => *k,
            None => 0,
        };
        Ok(r)
    }
    async fn delete_before(&self, r: u64) -> Result<()> {
        let ls: Vec<u64> = self.entries.read().await.range(..r).map(|x| *x.0).collect();
        for i in ls {
            self.entries.write().await.remove(&i);
        }
        let ls: Vec<u64> = self.tags.read().await.range(..r).map(|x| *x.0).collect();
        for i in ls {
            self.tags.write().await.remove(&i);
        }
        Ok(())
    }
    async fn insert_snapshot(&self, i: Index, e: Entry) -> Result<()> {
        unreachable!()
    }
    async fn insert_entry(&self, i: Index, e: Entry) -> Result<()> {
        self.entries.write().await.insert(i, e);
        Ok(())
    }
    async fn get_entry(&self, i: Index) -> Result<Option<Entry>> {
        let r = self.entries.read().await.get(&i).cloned();
        Ok(r)
    }
    async fn get_snapshot_index(&self) -> Result<Index> {
        unreachable!()
    }
    async fn save_ballot(&self, v: Ballot) -> Result<()> {
        *self.ballot.lock().await = v;
        Ok(())
    }
    async fn load_ballot(&self) -> Result<Ballot> {
        let r = self.ballot.lock().await.clone();
        Ok(r)
    }
}

#[tokio::test]
async fn test_mem_storage() -> Result<()> {
    let s = Storage::new();
    super::test_storage(s).await?;
    Ok(())
}
