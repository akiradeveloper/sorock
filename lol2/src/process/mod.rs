use super::*;

use anyhow::ensure;
use anyhow::Result;
use log::*;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::collections::HashSet;

use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;

mod api;
pub mod snapshot;
pub(crate) use api::*;
mod peer_svc;
use peer_svc::PeerSvc;
mod command_log;
use command_log::CommandLog;
mod voter;
use voter::Voter;
mod query_queue;
use query_queue::QueryQueue;
mod app;
use app::App;

mod command;
mod completion;
mod kern_message;
use command::Command;
use completion::*;
mod raft_process;
pub use raft_process::RaftProcess;
mod thread;

pub type Term = u64;
pub type Index = u64;

#[derive(Clone, Copy, Eq, Debug)]
pub struct Clock {
    pub term: Term,
    pub index: Index,
}
impl PartialEq for Clock {
    fn eq(&self, that: &Self) -> bool {
        self.term == that.term && self.index == that.index
    }
}

#[derive(Clone)]
pub struct Entry {
    pub prev_clock: Clock,
    pub this_clock: Clock,
    pub command: Bytes,
}

#[derive(Clone, Debug, PartialEq)]
pub struct Ballot {
    pub cur_term: Term,
    pub voted_for: Option<NodeId>,
}
impl Ballot {
    pub fn new() -> Self {
        Self {
            cur_term: 0,
            voted_for: None,
        }
    }
}

pub(crate) struct LogStream {
    pub sender_id: NodeId,
    pub prev_clock: Clock,
    pub entries: std::pin::Pin<Box<dyn futures::stream::Stream<Item = LogStreamElem> + Send>>,
}
pub(crate) struct LogStreamElem {
    pub this_clock: Clock,
    pub command: Bytes,
}

#[derive(shrinkwraprs::Shrinkwrap, Clone)]
struct Ref<T>(T);

/// `RaftApp` is the representation of state machine in Raft.
/// Beside the application state, it also contains the snapshot store
/// where snapshot data is stored with a snapshot index as a key.
#[async_trait::async_trait]
pub trait RaftApp: Sync + Send + 'static {
    /// Apply read request to the application.
    /// Calling of this function should not change the state of the application.
    async fn process_read(&self, request: &[u8]) -> Result<Bytes>;

    /// Apply write request to the application.
    /// Calling of this function may change the state of the application.
    async fn process_write(&self, request: &[u8], entry_index: Index) -> Result<Bytes>;

    /// Replace the state of the application with the snapshot.
    /// The snapshot is guaranteed to exist in the snapshot store.
    async fn install_snapshot(&self, snapshot_index: Index) -> Result<()>;

    /// Save snapshot with index `snapshot_index` to the snapshot store.
    /// This function is called when the snapshot is fetched from the leader.
    async fn save_snapshot(&self, st: snapshot::SnapshotStream, snapshot_index: Index) -> Result<()>;

    /// Read existing snapshot with index `snapshot_index` from the snapshot store.
    /// This function is called when a follower requests a snapshot from the leader.
    async fn open_snapshot(&self, snapshot_index: Index) -> Result<snapshot::SnapshotStream>;

    /// Delete all the snapshots in range [,  i) from the snapshot store.
    async fn delete_snapshots_before(&self, i: Index) -> Result<()>;

    /// Get the index of the latest snapshot in the snapshot store.
    /// If the index is greater than the current snapshot entry index,
    /// it will replace the snapshot entry with the new one.
    async fn get_latest_snapshot(&self) -> Result<Index>;
}

/// `RaftLogStore` is the representation of the log store in Raft.
/// Conceptually, it is like `RwLock<BTreeMap<Index, Entry>>`.
#[async_trait::async_trait]
pub trait RaftLogStore: Sync + Send + 'static {
    /// Insert the entry at index `i` into the log.
    async fn insert_entry(&self, i: Index, e: Entry) -> Result<()>;

    /// Delete all the entries in range [, i) from the log.
    async fn delete_entries_before(&self, i: Index) -> Result<()>;

    /// Get the entry at index `i` from the log.
    async fn get_entry(&self, i: Index) -> Result<Option<Entry>>;

    /// Get the index of the first entry in the log.
    async fn get_head_index(&self) -> Result<Index>;

    /// Get the index of the last entry in the log.
    async fn get_last_index(&self) -> Result<Index>;
}

/// `RaftBallotStore` is the representation of the ballot store in Raft.
/// Conceptually, it is like `RwLock<Ballot>`.
#[async_trait::async_trait]
pub trait RaftBallotStore: Sync + Send + 'static {
    /// Replace the current ballot with the new one.
    async fn save_ballot(&self, v: Ballot) -> Result<()>;

    /// Get the current ballot.
    async fn load_ballot(&self) -> Result<Ballot>;
}
