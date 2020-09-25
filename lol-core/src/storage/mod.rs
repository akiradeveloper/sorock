use crate::{Clock, Term, Index, Id};
use std::time::Instant;

pub mod memory;

#[cfg(feature = "persistency")]
pub mod disk;

#[derive(Clone)]
pub struct Vote {
    pub(crate) cur_term: Term,
    pub(crate) voted_for: Option<Id>,
}
impl Vote {
    fn new() -> Self {
        Self {
            cur_term: 0,
            voted_for: None,
        }
    }
}

#[derive(Clone)]
pub struct Entry {
    /// when this entry was inserted in this node
    pub(crate) append_time: Instant,
    pub(crate) prev_clock: Clock,
    pub(crate) this_clock: Clock,
    pub(crate) command: Vec<u8>,
}

// TODO error handling
#[async_trait::async_trait]
pub trait RaftStorage: Sync + Send + 'static {
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