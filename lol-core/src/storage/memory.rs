use super::{Ballot, Entry};
use crate::Index;
use anyhow::Result;
use std::collections::BTreeMap;
use std::sync::Arc;
use tokio::sync::{Mutex, RwLock};

pub struct Storage {
    entries: Arc<RwLock<BTreeMap<u64, super::Entry>>>,
    ballot: Arc<Mutex<Ballot>>,
}
impl Storage {
    pub fn new() -> Self {
        Self {
            entries: Arc::new(RwLock::new(BTreeMap::new())),
            ballot: Arc::new(Mutex::new(Ballot::new())),
        }
    }
}
#[async_trait::async_trait]
impl super::RaftStorage for Storage {
    async fn get_head_index(&self) -> Result<Index> {
        let x = self.entries.read().await;
        let r = match x.iter().next() {
            Some((k, _)) => *k,
            None => 0,
        };
        Ok(r)
    }
    async fn get_last_index(&self) -> Result<Index> {
        let x = self.entries.read().await;
        let r = match x.iter().next_back() {
            Some((k, _)) => *k,
            None => 0,
        };
        Ok(r)
    }
    async fn delete_entry(&self, i: Index) -> Result<()> {
        self.entries.write().await.remove(&i);
        Ok(())
    }
    async fn insert_entry(&self, i: Index, e: Entry) -> Result<()> {
        self.entries.write().await.insert(i, e);
        Ok(())
    }
    async fn get_entry(&self, i: Index) -> Result<Option<Entry>> {
        let r = self.entries.read().await.get(&i).cloned();
        Ok(r)
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage;

    #[tokio::test]
    async fn test_mem_storage() -> Result<()> {
        let s = Storage::new();
        storage::test_storage(s).await?;
        Ok(())
    }
}
