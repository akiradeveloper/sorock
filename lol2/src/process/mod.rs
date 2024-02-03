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
pub use snapshot::SnapshotStream;

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

pub struct LogStream {
    pub sender_id: NodeId,
    pub prev_clock: Clock,
    pub entries: std::pin::Pin<Box<dyn futures::stream::Stream<Item = LogStreamElem> + Send>>,
}
pub struct LogStreamElem {
    pub this_clock: Clock,
    pub command: Bytes,
}

#[derive(shrinkwraprs::Shrinkwrap, Clone)]
struct Ref<T>(T);

#[async_trait::async_trait]
pub trait RaftApp: Sync + Send + 'static {
    async fn process_read(&self, request: &[u8]) -> Result<Bytes>;
    async fn process_write(&self, request: &[u8], entry_index: Index) -> Result<Bytes>;
    async fn install_snapshot(&self, snapshot: Index) -> Result<()>;
    async fn save_snapshot(&self, st: SnapshotStream, snapshot_index: Index) -> Result<()>;
    async fn open_snapshot(&self, x: Index) -> Result<SnapshotStream>;
    async fn delete_snapshots_before(&self, x: Index) -> Result<()>;
    async fn get_latest_snapshot(&self) -> Result<Index>;
}

#[async_trait::async_trait]
pub trait RaftLogStore: Sync + Send + 'static {
    async fn insert_entry(&self, i: Index, e: Entry) -> Result<()>;
    async fn delete_entries_before(&self, i: Index) -> Result<()>;
    async fn get_entry(&self, i: Index) -> Result<Option<Entry>>;
    async fn get_head_index(&self) -> Result<Index>;
    async fn get_last_index(&self) -> Result<Index>;
}

#[async_trait::async_trait]
pub trait RaftBallotStore: Sync + Send + 'static {
    async fn save_ballot(&self, v: Ballot) -> Result<()>;
    async fn load_ballot(&self) -> Result<Ballot>;
}
