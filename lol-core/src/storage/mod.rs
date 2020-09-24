use crate::{Clock, Term, Index, Id};
use std::time::Instant;

pub mod memory;

#[cfg(feature = "persistency")]
pub mod disk;

#[derive(Clone)]
pub struct Vote {
    pub cur_term: Term,
    pub voted_for: Option<Id>,
}
impl Vote {
    pub fn new() -> Self {
        Self {
            cur_term: 0,
            voted_for: None,
        }
    }
}

#[derive(Clone)]
pub struct Entry {
    /// when this entry was inserted in this node
    pub append_time: Instant,
    pub prev_clock: Clock,
    pub this_clock: Clock,
    pub command: Vec<u8>,
}

// TODO error handling
#[async_trait::async_trait]
pub trait RaftStorage {
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