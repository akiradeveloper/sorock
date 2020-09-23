use crate::{Clock, Term, Index, Id, Entry, Vote};
use std::time::Instant;

pub mod memory;
pub mod disk;

// TODO error handling
#[async_trait::async_trait]
trait RaftStorage {
    /// delete ..r
    async fn delete_before(&self, r: Index);
    /// save snapshot entry and forward snapshot_index atomically
    async fn insert_snapshot(&self, i: Index, e: Entry);
    async fn insert_entry(&self, i: Index, e: Entry);
    async fn get_entry(&self, i: Index) -> Option<Entry>;
    async fn get_snapshot_index(&self) -> Index;
    async fn get_last_index(&self) -> Index;
    async fn store_vote(&self, v: Vote);
    async fn load_vote(&self) -> Vote;
}