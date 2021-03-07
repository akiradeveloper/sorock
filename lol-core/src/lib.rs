#![deny(unused_must_use)]
#![cfg_attr(docsrs, feature(doc_cfg))]

//! Raft is a distributed consensus algorithm widely used nowadays
//! to build distributed applications like etcd.
//! However, while it is even understandable than notorious Paxos algorithm
//! it is still difficult to implement correct and efficient implementation.
//!
//! This library is a Raft implementation based on Tonic, a gRPC library based
//! on Tokio.
//! By exploiting gRPC features like streaming, the inter-node log replication
//! and snapshot copying is very efficient.
//! Also, zero-copy ser/desr between replication stream and the log entries is
//! another goal of this library in term of efficiency.

use anyhow::anyhow;
use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::StreamExt;
use std::collections::{BTreeMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{Mutex, Notify, RwLock, Semaphore};

mod ack;
/// Simple and backward-compatible RaftApp trait.
pub mod compat;
/// Utilities for connection.
pub mod connection;
/// The request and response that RaftCore talks.
pub mod core_message;
mod membership;
mod query_queue;
mod quorum_join;
mod server;
/// The snapshot abstraction and some basic implementations.
pub mod snapshot;
/// The abstraction for the backing storage and some implementations.
pub mod storage;
mod thread;
mod thread_drop;

use ack::Ack;
use connection::Endpoint;
use snapshot::SnapshotTag;
use storage::RaftStorage;

/// Proto file compiled.
pub mod proto_compiled {
    tonic::include_proto!("lol_core");
}

use storage::{Ballot, Entry};

/// Plan to make a new snapshot.
pub enum MakeSnapshot {
    /// No snapshot will be made.
    None,
    /// Copy snapshot will be made.
    CopySnapshot(SnapshotTag),
    /// Fold snapshot will be made.
    FoldSnapshot,
}

/// The abstraction for user-defined application runs on the RaftCore.
#[async_trait]
pub trait RaftApp: Sync + Send + 'static {
    /// How state machine interacts with inputs from clients.
    async fn process_message(&self, request: &[u8]) -> anyhow::Result<Vec<u8>>;
    /// Almost same as process_message but is called in log application path.
    /// This function may return `MakeSnapshot` to make a new snapshot.
    /// Note that the snapshot entry corresponding to the copy snapshot is not guaranteed to be made
    /// due to possible I/O errors, etc.
    async fn apply_message(
        &self,
        request: &[u8],
        apply_index: Index,
    ) -> anyhow::Result<(Vec<u8>, MakeSnapshot)>;
    /// Special type of apply_message but when the entry is snapshot entry.
    /// Snapshot is None happens iff apply_index is 1 which is the most initial snapshot.
    async fn install_snapshot(
        &self,
        snapshot: Option<&SnapshotTag>,
        apply_index: Index,
    ) -> anyhow::Result<()>;
    /// This function is called from compaction threads.
    /// It should return new snapshot from accumulative compution with the old_snapshot and the subsequent log entries.
    async fn fold_snapshot(
        &self,
        old_snapshot: Option<&SnapshotTag>,
        requests: Vec<&[u8]>,
    ) -> anyhow::Result<SnapshotTag>;
    /// Make a snapshot resource and returns the tag.
    async fn from_snapshot_stream(
        &self,
        st: snapshot::SnapshotStream,
        snapshot_index: Index,
    ) -> anyhow::Result<SnapshotTag>;
    /// Make a snapshot stream from a snapshot resource bound to the tag.
    async fn to_snapshot_stream(&self, x: &SnapshotTag) -> snapshot::SnapshotStream;
    /// Delete a snapshot resource bound to the tag.
    async fn delete_resource(&self, x: &SnapshotTag) -> anyhow::Result<()>;
}

type Term = u64;
/// Log index.
pub type Index = u64;
#[derive(Clone, Copy, Eq)]
struct Clock {
    term: Term,
    index: Index,
}
impl PartialEq for Clock {
    fn eq(&self, that: &Self) -> bool {
        self.term == that.term && self.index == that.index
    }
}

/// Unique identifier of a Raft node.
///
/// Id must satisfy these two conditions:
/// 1. Id can identify a node in the cluster.
/// 2. any client or other nodes in the cluster can access this node by the Id.
///
/// Typically, the form of Id is (http|https)://(hostname|ip):port
pub type Id = String;

#[derive(serde::Serialize, serde::Deserialize)]
enum Command<'a> {
    Noop,
    Snapshot {
        membership: HashSet<Id>,
    },
    ClusterConfiguration {
        membership: HashSet<Id>,
    },
    Req {
        core: bool,
        #[serde(with = "serde_bytes")]
        message: &'a [u8],
    },
}
impl<'a> Command<'a> {
    fn serialize(x: &Command) -> Bytes {
        bincode::serialize(x).unwrap().into()
    }
    fn deserialize(x: &[u8]) -> Command {
        bincode::deserialize(x).unwrap()
    }
}
#[derive(Clone, Copy)]
enum ElectionState {
    Leader,
    Candidate,
    Follower,
}
/// Static configuration in initialization.
pub struct Config {
    id: Id,
}
impl Config {
    pub fn new(id: Id) -> Self {
        Self { id }
    }
}

/// Dynamic configurations.
pub struct TunableConfig {
    /// Snapshot will be inserted into log after this delay.
    pub compaction_delay_sec: u64,
    /// The interval that compaction runs.
    /// You can set this to 0 and fold snapshot will never be created.
    pub compaction_interval_sec: u64,
}
impl TunableConfig {
    pub fn default() -> Self {
        Self {
            compaction_delay_sec: 150,
            compaction_interval_sec: 300,
        }
    }
}
struct FailureDetector {
    watch_id: Id,
    detector: phi_detector::PingWindow,
}
impl FailureDetector {
    fn watch(id: Id) -> Self {
        Self {
            watch_id: id,
            detector: phi_detector::PingWindow::new(&[Duration::from_secs(1)], Instant::now()),
        }
    }
}

/// RaftCore is the heart of the Raft system.
/// It does everything Raft should do like election, dynamic membership change,
/// log replication, sending snapshot in stream and interaction with user-defined RaftApp.
pub struct RaftCore<A: RaftApp> {
    id: Id,
    app: A,
    query_queue: Mutex<query_queue::QueryQueue>,
    log: Log,
    election_state: RwLock<ElectionState>,
    cluster: RwLock<membership::Cluster>,
    tunable: RwLock<TunableConfig>,
    vote_token: Semaphore,
    // Until noop is committed and safe term is incrememted
    // no new entry in the current term is appended to the log.
    safe_term: AtomicU64,
    // Membership should not be appended until commit_index passes this line.
    membership_barrier: AtomicU64,

    failure_detector: RwLock<FailureDetector>,
}
impl<A: RaftApp> RaftCore<A> {
    pub async fn new<S: RaftStorage>(
        app: A,
        storage: S,
        config: Config,
        tunable: TunableConfig,
    ) -> Arc<Self> {
        let id = config.id;
        let init_cluster = membership::Cluster::empty(id.clone()).await;
        let (membership_index, init_membership) =
            Self::find_last_membership(&storage).await.unwrap();
        let init_log = Log::new(Box::new(storage)).await;
        let fd = FailureDetector::watch(id.clone());
        let r = Arc::new(Self {
            app,
            query_queue: Mutex::new(query_queue::QueryQueue::new()),
            id,
            log: init_log,
            election_state: RwLock::new(ElectionState::Follower),
            cluster: RwLock::new(init_cluster),
            tunable: RwLock::new(tunable),
            vote_token: Semaphore::new(1),
            safe_term: 0.into(),
            membership_barrier: 0.into(),
            failure_detector: RwLock::new(fd),
        });
        log::info!(
            "initial membership is {:?} at {}",
            init_membership,
            membership_index
        );
        r.set_membership(&init_membership, membership_index)
            .await
            .unwrap();
        r
    }
    async fn find_last_membership<S: RaftStorage>(
        storage: &S,
    ) -> anyhow::Result<(Index, HashSet<Id>)> {
        let from = storage.get_snapshot_index().await?;
        if from == 0 {
            return Ok((0, HashSet::new()));
        }
        let to = storage.get_last_index().await?;
        assert!(from <= to);
        let mut ret = (0, HashSet::new());
        for i in from..=to {
            let e = storage.get_entry(i).await?.unwrap();
            match Command::deserialize(&e.command) {
                Command::Snapshot { membership } => {
                    ret = (i, membership);
                }
                Command::ClusterConfiguration { membership } => {
                    ret = (i, membership);
                }
                _ => {}
            }
        }
        Ok(ret)
    }
    async fn init_cluster(self: &Arc<Self>) -> anyhow::Result<()> {
        let snapshot = Entry {
            prev_clock: Clock { term: 0, index: 0 },
            this_clock: Clock { term: 0, index: 1 },
            command: Command::serialize(&Command::Snapshot {
                membership: HashSet::new(),
            }),
        };
        self.log.insert_snapshot(snapshot).await?;
        let mut membership = HashSet::new();
        membership.insert(self.id.clone());
        let add_server = Entry {
            prev_clock: Clock { term: 0, index: 1 },
            this_clock: Clock { term: 0, index: 2 },
            command: Command::serialize(&Command::ClusterConfiguration {
                membership: membership.clone(),
            }),
        };
        self.log.insert_entry(add_server).await?;
        self.set_membership(&membership, 2).await?;

        // After this function is called
        // this server immediately becomes the leader by self-vote and advance commit index.
        // Consequently, when initial install_snapshot is called this server is already the leader.
        self.send_timeout_now(self.id.clone());

        Ok(())
    }
    fn allow_new_membership_change(&self) -> bool {
        self.log.commit_index.load(Ordering::SeqCst)
            >= self.membership_barrier.load(Ordering::SeqCst)
    }
    async fn set_membership(
        self: &Arc<Self>,
        membership: &HashSet<Id>,
        index: Index,
    ) -> anyhow::Result<()> {
        log::info!("change membership to {:?}", membership);
        self.cluster
            .write()
            .await
            .set_membership(&membership, Arc::clone(&self))
            .await?;
        self.membership_barrier.store(index, Ordering::SeqCst);
        Ok(())
    }
    async fn process_message(self: &Arc<Self>, msg: &[u8]) -> anyhow::Result<Vec<u8>> {
        let req = core_message::Req::deserialize(msg).unwrap();
        match req {
            core_message::Req::ClusterInfo => {
                let res = core_message::Rep::ClusterInfo {
                    leader_id: self.load_ballot().await?.voted_for,
                    membership: {
                        let membership = self.cluster.read().await.membership.clone();
                        let mut xs: Vec<_> = membership.into_iter().collect();
                        xs.sort();
                        xs
                    },
                };
                Ok(core_message::Rep::serialize(&res))
            }
            core_message::Req::LogInfo => {
                let res = core_message::Rep::LogInfo {
                    snapshot_index: self.log.get_snapshot_index().await?,
                    last_applied: self.log.last_applied.load(Ordering::SeqCst),
                    commit_index: self.log.commit_index.load(Ordering::SeqCst),
                    last_log_index: self.log.get_last_log_index().await?,
                };
                Ok(core_message::Rep::serialize(&res))
            }
            core_message::Req::HealthCheck => {
                let res = core_message::Rep::HealthCheck { ok: true };
                Ok(core_message::Rep::serialize(&res))
            }
            _ => Err(anyhow!("the message not supported")),
        }
    }
    async fn register_query(self: &Arc<Self>, core: bool, message: Bytes, ack: Ack) {
        let query = query_queue::Query { core, message, ack };
        self.query_queue
            .lock()
            .await
            .register(self.log.commit_index.load(Ordering::SeqCst), query);
        // In case last_applied == commit_index and there is no subsequent entries after this line,
        // no notification on last_applied's change will be made and this query will never be processed.
        // To avoid this, here manually kicks the execution of query_queue.
        self.query_queue
            .lock()
            .await
            .execute(
                self.log.last_applied.load(Ordering::SeqCst),
                Arc::clone(&self),
            )
            .await;
    }
}
enum TryInsertResult {
    Inserted,
    Skipped,
    Rejected,
}
struct LogStream {
    sender_id: String,
    prev_log_term: Term,
    prev_log_index: Index,
    entries: std::pin::Pin<Box<dyn futures::stream::Stream<Item = LogStreamElem> + Send + Sync>>,
}
struct LogStreamElem {
    term: Term,
    index: Index,
    command: Bytes,
}
// Wrapper to safely add `Sync` to stream.
//
// This wrapper is a work-around to issues like
// - https://github.com/dtolnay/async-trait/issues/77
// - https://github.com/hyperium/hyper/pull/2187
//
// In our case, the problem is tonic::IntoStreamingRequest requires the given stream to be `Sync`.
// Unless async_trait starts to return `Future`s with `Sync` or tonic (or maybe hyper under the hood) is fixed this wrapper should remain.
struct SyncStream<S> {
    st: sync_wrapper::SyncWrapper<S>,
}
impl<S> SyncStream<S> {
    fn new(st: S) -> Self {
        Self {
            st: sync_wrapper::SyncWrapper::new(st),
        }
    }
}
impl<S: futures::stream::Stream> futures::stream::Stream for SyncStream<S> {
    type Item = S::Item;
    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut futures::task::Context<'_>,
    ) -> futures::task::Poll<Option<Self::Item>> {
        let st = unsafe { self.map_unchecked_mut(|x| x.st.get_mut()) };
        st.poll_next(cx)
    }
}
fn into_out_stream(
    x: LogStream,
) -> impl futures::stream::Stream<Item = crate::proto_compiled::AppendEntryReq> {
    use crate::proto_compiled::{append_entry_req::Elem, AppendStreamEntry, AppendStreamHeader};
    let header_stream = vec![Elem::Header(AppendStreamHeader {
        sender_id: x.sender_id,
        prev_log_index: x.prev_log_index,
        prev_log_term: x.prev_log_term,
    })];
    let header_stream = futures::stream::iter(header_stream);
    let chunk_stream = x.entries.map(|e| {
        Elem::Entry(AppendStreamEntry {
            term: e.term,
            index: e.index,
            command: e.command,
        })
    });
    header_stream
        .chain(chunk_stream)
        .map(|e| crate::proto_compiled::AppendEntryReq { elem: Some(e) })
}
// Replication
impl<A: RaftApp> RaftCore<A> {
    async fn change_membership(
        self: &Arc<Self>,
        command: Bytes,
        index: Index,
    ) -> anyhow::Result<()> {
        match Command::deserialize(&command) {
            Command::Snapshot { membership } => {
                self.set_membership(&membership, index).await?;
            }
            Command::ClusterConfiguration { membership } => {
                self.set_membership(&membership, index).await?;
            }
            _ => {}
        }
        Ok(())
    }
    fn commit_safe_term(&self, term: Term) {
        log::info!("noop entry for term {} is successfully committed", term);
        self.safe_term.fetch_max(term, Ordering::SeqCst);
    }
    // Leader calls this fucntion to append new entry to its log.
    async fn queue_entry(self: &Arc<Self>, command: Bytes, ack: Option<Ack>) -> anyhow::Result<()> {
        let term = self.load_ballot().await?.cur_term;
        // safe_term is a term that noop entry is successfully committed.
        let cur_safe_term = self.safe_term.load(Ordering::SeqCst);
        if cur_safe_term < term {
            return Err(anyhow!(
                "noop entry for term {} isn't committed yet. (> {})",
                term,
                cur_safe_term
            ));
        }
        // command.clone() is cheap because the message buffer is Bytes.
        let append_index = self
            .log
            .append_new_entry(command.clone(), ack, term)
            .await?;
        self.log.replication_notify.notify_one();

        // Change membership when cluster configuration is appended.
        self.change_membership(command, append_index).await?;
        Ok(())
    }
    // Follower calls this function when it receives entries from the leader.
    async fn queue_received_entry(self: &Arc<Self>, mut req: LogStream) -> anyhow::Result<bool> {
        let mut prev_clock = Clock {
            term: req.prev_log_term,
            index: req.prev_log_index,
        };
        while let Some(e) = req.entries.next().await {
            let entry = Entry {
                prev_clock,
                this_clock: Clock {
                    term: e.term,
                    index: e.index,
                },
                command: e.command,
            };
            let insert_index = entry.this_clock.index;
            let command = entry.command.clone();
            match self
                .log
                .try_insert_entry(entry, req.sender_id.clone(), Arc::clone(&self))
                .await?
            {
                TryInsertResult::Inserted => {
                    self.change_membership(command, insert_index).await?;
                }
                TryInsertResult::Skipped => {}
                TryInsertResult::Rejected => {
                    log::warn!("rejected append entry (clock={:?})", (e.term, e.index));
                    return Ok(false);
                }
            }
            prev_clock = Clock {
                term: e.term,
                index: e.index,
            };
        }
        Ok(true)
    }
    async fn prepare_replication_stream(
        self: &Arc<Self>,
        l: Index,
        r: Index,
    ) -> anyhow::Result<LogStream> {
        let head = self.log.storage.get_entry(l).await?.unwrap();
        let Clock {
            term: prev_log_term,
            index: prev_log_index,
        } = head.prev_clock;
        let Clock { term, index } = head.this_clock;
        let e = LogStreamElem {
            term,
            index,
            command: head.command,
        };
        let st1 = futures::stream::iter(vec![e]);
        let core = Arc::clone(&self);
        let st2 = async_stream::stream! {
            for idx in l + 1..r {
                let x = core.log.storage.get_entry(idx).await.unwrap().unwrap();
                let Clock { term, index } = x.this_clock;
                let e = LogStreamElem {
                    term,
                    index,
                    command: x.command,
                };
                yield e;
            }
        };
        let st2 = SyncStream::new(st2);
        let st = st1.chain(st2);
        Ok(LogStream {
            sender_id: self.id.clone(),
            prev_log_term,
            prev_log_index,
            entries: Box::pin(st),
        })
    }
    async fn advance_replication(self: &Arc<Self>, follower_id: Id) -> anyhow::Result<bool> {
        let peer = self
            .cluster
            .read()
            .await
            .peers
            .get(&follower_id)
            .unwrap()
            .clone();

        let old_progress = peer.progress;
        let cur_last_log_index = self.log.get_last_log_index().await?;

        // More entries to send?
        let should_send = cur_last_log_index >= old_progress.next_index;
        if !should_send {
            return Ok(false);
        }

        // The entries to send could be deleted due to previous compactions.
        // In this case, replication will reset from the current snapshot index.
        let cur_snapshot_index = self.log.get_snapshot_index().await?;
        if old_progress.next_index < cur_snapshot_index {
            log::warn!(
                "entry not found at next_index (idx={}) for {}",
                old_progress.next_index,
                follower_id
            );
            let new_progress = membership::ReplicationProgress::new(cur_snapshot_index);
            let mut cluster = self.cluster.write().await;
            cluster.peers.get_mut(&follower_id).unwrap().progress = new_progress;
            return Ok(true);
        }

        let n_max_possible = cur_last_log_index - old_progress.next_index + 1;
        let n = std::cmp::min(old_progress.next_max_cnt, n_max_possible);
        assert!(n >= 1);

        let in_stream = self
            .prepare_replication_stream(old_progress.next_index, old_progress.next_index + n)
            .await?;

        let res = async {
            let endpoint = Endpoint::from_shared(follower_id.clone()).unwrap();
            let mut conn = connection::connect(endpoint).await?;
            let out_stream = into_out_stream(in_stream);
            conn.send_append_entry(out_stream).await
        }
        .await;

        let mut incremented = false;
        let new_progress = if res.is_ok() {
            let res = res.unwrap();
            match res.into_inner() {
                crate::proto_compiled::AppendEntryRep { success: true, .. } => {
                    incremented = true;
                    membership::ReplicationProgress {
                        match_index: old_progress.next_index + n - 1,
                        next_index: old_progress.next_index + n,
                        next_max_cnt: n * 2,
                    }
                }
                crate::proto_compiled::AppendEntryRep {
                    success: false,
                    last_log_index,
                } => membership::ReplicationProgress {
                    match_index: old_progress.match_index,
                    next_index: std::cmp::min(old_progress.next_index - 1, last_log_index + 1),
                    next_max_cnt: 1,
                },
            }
        } else {
            old_progress
        };

        {
            let mut cluster = self.cluster.write().await;
            cluster.peers.get_mut(&follower_id).unwrap().progress = new_progress;
        }
        if incremented {
            self.log.replication_notify.notify_one();
        }

        Ok(true)
    }
    async fn find_new_agreement(&self) -> anyhow::Result<Index> {
        let mut match_indices = vec![];

        // In leader stepdown, leader is out of the membership
        // but consensus on the membership change should be made to respond to the client.
        let last_log_index = self.log.get_last_log_index().await?;
        match_indices.push(last_log_index);

        for (_, peer) in self.cluster.read().await.peers.clone() {
            match_indices.push(peer.progress.match_index);
        }

        match_indices.sort();
        match_indices.reverse();
        let mid = match_indices.len() / 2;
        let new_agreement = match_indices[mid];
        Ok(new_agreement)
    }
}
// Snapshot
impl<A: RaftApp> RaftCore<A> {
    async fn fetch_snapshot(&self, snapshot_index: Index, to: Id) -> anyhow::Result<()> {
        // TODO: Setting connection timeout can be appropriate
        //
        // Fetching snapshot can take very long then setting timeout is not appropriate here.
        let endpoint = Endpoint::from_shared(to).unwrap();
        let mut conn = connection::connect(endpoint).await?;
        let req = proto_compiled::GetSnapshotReq {
            index: snapshot_index,
        };
        let res = conn.get_snapshot(req).await?;
        let out_stream = res.into_inner();
        let in_stream = Box::pin(snapshot::into_in_stream(out_stream));
        let tag = self
            .app
            .from_snapshot_stream(in_stream, snapshot_index)
            .await?;
        self.log.storage.put_tag(snapshot_index, tag).await?;
        Ok(())
    }
    async fn make_snapshot_stream(
        &self,
        snapshot_index: Index,
    ) -> anyhow::Result<Option<snapshot::SnapshotStream>> {
        let tag = self.log.storage.get_tag(snapshot_index).await?;
        if tag.is_none() {
            return Ok(None);
        }
        let tag = tag.unwrap();
        let st = self.app.to_snapshot_stream(&tag).await;
        Ok(Some(st))
    }
}
// Election
impl<A: RaftApp> RaftCore<A> {
    async fn save_ballot(&self, v: Ballot) -> anyhow::Result<()> {
        self.log.storage.save_ballot(v).await
    }
    async fn load_ballot(&self) -> anyhow::Result<Ballot> {
        self.log.storage.load_ballot().await
    }
    async fn detect_election_timeout(&self) -> bool {
        let fd = &self.failure_detector.read().await.detector;
        let normal_dist = fd.normal_dist();
        let phi = normal_dist.phi(Instant::now() - fd.last_ping());
        phi > 3.
    }
    async fn receive_vote(
        &self,
        candidate_term: Term,
        candidate_id: Id,
        candidate_last_log_clock: Clock,
        force_vote: bool,
        pre_vote: bool,
    ) -> anyhow::Result<bool> {
        let allow_side_effects = !pre_vote;

        if !force_vote {
            if !self.detect_election_timeout().await {
                return Ok(false);
            }
        }

        let _vote_guard = self.vote_token.acquire().await;

        let mut ballot = self.load_ballot().await?;
        if candidate_term < ballot.cur_term {
            log::warn!("candidate term is older. reject vote");
            return Ok(false);
        }

        if candidate_term > ballot.cur_term {
            log::warn!("received newer term. reset vote");
            ballot.cur_term = candidate_term;
            ballot.voted_for = None;
            if allow_side_effects {
                *self.election_state.write().await = ElectionState::Follower;
            }
        }

        let cur_last_index = self.log.get_last_log_index().await?;

        // Suppose we have 3 in-memory nodes ND0-2 and initially ND0 is the leader,
        // log is fully replicated between nodes and there is no in-coming entries.
        // Suddenly, ND0 and 1 is crashed and soon later rebooted.
        // In this case, ND2 should become leader by getting vote from either ND0 or ND1.
        // This is why using weakest clock (0,0) here when there is no entry in the log.
        let this_last_log_clock = self
            .log
            .storage
            .get_entry(cur_last_index)
            .await?
            .map(|x| x.this_clock)
            .unwrap_or(Clock { term: 0, index: 0 });

        let candidate_win = match candidate_last_log_clock.term.cmp(&this_last_log_clock.term) {
            std::cmp::Ordering::Greater => true,
            std::cmp::Ordering::Equal => {
                candidate_last_log_clock.index >= this_last_log_clock.index
            }
            std::cmp::Ordering::Less => false,
        };

        if !candidate_win {
            log::warn!("candidate clock is older. reject vote");
            if allow_side_effects {
                self.save_ballot(ballot).await?;
            }
            return Ok(false);
        }

        let grant = match &ballot.voted_for {
            None => {
                ballot.voted_for = Some(candidate_id.clone());
                true
            }
            Some(id) => {
                if id == &candidate_id {
                    true
                } else {
                    false
                }
            }
        };

        if allow_side_effects {
            self.save_ballot(ballot).await?;
        }
        log::info!("voted response to {} = grant: {}", candidate_id, grant);
        Ok(grant)
    }
    async fn request_votes(
        self: &Arc<Self>,
        aim_term: Term,
        force_vote: bool,
        pre_vote: bool,
    ) -> anyhow::Result<bool> {
        let (others, remaining) = {
            let membership = self.cluster.read().await.membership.clone();
            let n = membership.len();
            let majority = (n / 2) + 1;
            let include_self = membership.contains(&self.id);
            let mut others = vec![];
            for id in membership {
                if id != self.id {
                    others.push(id);
                }
            }
            // -1 = self vote
            let m = if include_self { majority - 1 } else { majority };
            (others, m)
        };

        let last_log_index = self.log.get_last_log_index().await?;
        let last_log_clock = self
            .log
            .storage
            .get_entry(last_log_index)
            .await?
            .unwrap()
            .this_clock;

        let vote_timeout = Duration::from_secs(5);

        // Let's get remaining votes out of others.
        let mut vote_requests = vec![];
        for endpoint in others {
            let myid = self.id.clone();
            vote_requests.push(async move {
                let Clock {
                    term: last_log_term,
                    index: last_log_index,
                } = last_log_clock;
                let req = crate::proto_compiled::RequestVoteReq {
                    term: aim_term,
                    candidate_id: myid,
                    last_log_term,
                    last_log_index,
                    // $4.2.3
                    // If force_vote is set, the receiver server accepts the vote request
                    // regardless of the heartbeat timeout otherwise the vote request is
                    // dropped when it's receiving heartbeat.
                    force_vote,
                    // $9.6 Preventing disruptions when a server rejoins the cluster
                    // We recommend the Pre-Vote extension in deployments that would benefit from additional robustness.
                    pre_vote,
                };
                let res = async {
                    let endpoint = Endpoint::from_shared(endpoint)
                        .unwrap()
                        .timeout(vote_timeout);
                    let mut conn = connection::connect(endpoint).await?;
                    conn.request_vote(req).await
                }
                .await;
                match res {
                    Ok(res) => res.into_inner().vote_granted,
                    Err(_) => false,
                }
            });
        }
        let ok = quorum_join::quorum_join(remaining, vote_requests).await;
        Ok(ok)
    }
    async fn after_votes(self: &Arc<Self>, aim_term: Term, ok: bool) -> anyhow::Result<()> {
        if ok {
            log::info!("got enough votes from the cluster. promoted to leader");

            // As soon as the node becomes the leader, replicate noop entries with term.
            let index = self
                .log
                .append_new_entry(Command::serialize(&Command::Noop), None, aim_term)
                .await?;
            self.membership_barrier.store(index, Ordering::SeqCst);

            // Initialize replication progress
            {
                let initial_progress =
                    membership::ReplicationProgress::new(self.log.get_last_log_index().await?);
                let mut cluster = self.cluster.write().await;
                for (_, peer) in &mut cluster.peers {
                    peer.progress = initial_progress.clone();
                }
            }

            *self.election_state.write().await = ElectionState::Leader;
        } else {
            log::info!("failed to become leader. now back to follower");
            *self.election_state.write().await = ElectionState::Follower;
        }
        Ok(())
    }
    async fn try_promote(self: &Arc<Self>, force_vote: bool) -> anyhow::Result<()> {
        // Pre-Vote phase.
        let pre_aim_term = {
            let _ballot_guard = self.vote_token.acquire().await;
            let ballot = self.load_ballot().await?;
            ballot.cur_term + 1
        };

        log::info!("start pre-vote. try promote at term {}", pre_aim_term);
        let ok = self
            .request_votes(pre_aim_term, force_vote, true)
            .await
            .unwrap_or(false);
        // If pre-vote failed, do nothing and return.
        if !ok {
            log::info!("pre-vote failed for term {}", pre_aim_term);
            return Ok(());
        }

        // Vote to self
        let aim_term = {
            let ballot_guard = self.vote_token.acquire().await;
            let mut new_ballot = self.load_ballot().await?;
            let aim_term = new_ballot.cur_term + 1;

            // If the aim-term's changed, no election starts similar to compare-and-swap.
            // This could happen if this node's received TimeoutNow.
            if aim_term != pre_aim_term {
                return Ok(());
            }

            new_ballot.cur_term = aim_term;
            new_ballot.voted_for = Some(self.id.clone());

            self.save_ballot(new_ballot).await?;
            drop(ballot_guard);

            // Becoming Candidate avoids this node starts another election during this election.
            *self.election_state.write().await = ElectionState::Candidate;
            aim_term
        };

        log::info!("start election. try promote at term {}", aim_term);

        // Try to promote at the term.
        // Failing some I/O operations during election will be considered as election failure.
        let ok = self
            .request_votes(aim_term, force_vote, false)
            .await
            .unwrap_or(false);
        self.after_votes(aim_term, ok).await?;

        Ok(())
    }
    async fn send_heartbeat(&self, follower_id: Id) -> anyhow::Result<()> {
        let endpoint =
            connection::Endpoint::from_shared(follower_id.clone())?.timeout(Duration::from_secs(5));
        let req = {
            let term = self.load_ballot().await?.cur_term;
            proto_compiled::HeartbeatReq {
                term,
                leader_id: self.id.clone(),
                leader_commit: self.log.commit_index.load(Ordering::SeqCst),
            }
        };
        if let Ok(mut conn) = connection::connect(endpoint).await {
            let res = conn.send_heartbeat(req).await;
            if res.is_err() {
                log::warn!("heartbeat to {} failed", follower_id);
            }
        }
        Ok(())
    }
    async fn record_heartbeat(&self) {
        self.failure_detector
            .write()
            .await
            .detector
            .add_ping(Instant::now())
    }
    async fn reset_failure_detector(&self, leader_id: Id) {
        let cur_watch_id = self.failure_detector.read().await.watch_id.clone();
        if cur_watch_id != leader_id {
            *self.failure_detector.write().await = FailureDetector::watch(leader_id);
        }
    }
    async fn receive_heartbeat(
        self: &Arc<Self>,
        leader_id: Id,
        leader_term: Term,
        leader_commit: Index,
    ) -> anyhow::Result<()> {
        let ballot_guard = self.vote_token.acquire().await;
        let mut ballot = self.load_ballot().await?;
        if leader_term < ballot.cur_term {
            log::warn!("heartbeat is stale. rejected");
            return Ok(());
        }

        self.record_heartbeat().await;
        self.reset_failure_detector(leader_id.clone()).await;

        if leader_term > ballot.cur_term {
            log::warn!("received heartbeat with newer term. reset ballot");
            ballot.cur_term = leader_term;
            ballot.voted_for = None;
            *self.election_state.write().await = ElectionState::Follower;
        }

        if ballot.voted_for != Some(leader_id.clone()) {
            log::info!("learn the current leader ({})", leader_id);
            ballot.voted_for = Some(leader_id);
        }

        self.save_ballot(ballot).await?;
        drop(ballot_guard);

        let new_commit_index = std::cmp::min(leader_commit, self.log.get_last_log_index().await?);
        self.log
            .advance_commit_index(new_commit_index, Arc::clone(&self))
            .await?;

        Ok(())
    }
    async fn transfer_leadership(&self) {
        let mut xs = vec![];
        let peers = self.cluster.read().await.peers.clone();
        for (id, peer) in peers {
            let progress = peer.progress;
            xs.push((progress.match_index, id));
        }
        xs.sort_by_key(|x| x.0);

        // Choose the one with the higher match_index as the next leader.
        if let Some((_, id)) = xs.pop() {
            self.send_timeout_now(id);
        }
    }
    fn send_timeout_now(&self, id: Id) {
        tokio::spawn(async move {
            let endpoint = Endpoint::from_shared(id).unwrap();
            if let Ok(mut conn) = connection::connect(endpoint).await {
                let req = proto_compiled::TimeoutNowReq {};
                let _ = conn.timeout_now(req).await;
            }
        });
    }
}
struct Log {
    storage: Box<dyn RaftStorage>,
    ack_chans: RwLock<BTreeMap<Index, Ack>>,

    last_applied: AtomicU64, // Monotonic
    commit_index: AtomicU64, // Monotonic

    append_token: Semaphore,
    commit_token: Semaphore,
    compaction_token: Semaphore,

    append_notify: Notify,
    replication_notify: Notify,
    commit_notify: Notify,
    apply_notify: Notify,

    applied_membership: Mutex<HashSet<Id>>,
    snapshot_queue: snapshot::SnapshotQueue,

    apply_error_seq: AtomicU64,
}
impl Log {
    async fn new(storage: Box<dyn RaftStorage>) -> Self {
        let snapshot_index = storage.get_snapshot_index().await.unwrap();
        // When the storage is persistent initial commit_index and last_applied
        // should be set appropriately just before the snapshot index.
        let start_index = if snapshot_index == 0 {
            0
        } else {
            snapshot_index - 1
        };
        Self {
            storage,
            ack_chans: RwLock::new(BTreeMap::new()),

            last_applied: start_index.into(),
            commit_index: start_index.into(),

            append_token: Semaphore::new(1),
            commit_token: Semaphore::new(1),
            compaction_token: Semaphore::new(1),

            append_notify: Notify::new(),
            replication_notify: Notify::new(),
            commit_notify: Notify::new(),
            apply_notify: Notify::new(),

            applied_membership: Mutex::new(HashSet::new()),
            snapshot_queue: snapshot::SnapshotQueue::new(),

            apply_error_seq: 0.into(),
        }
    }
    async fn get_last_log_index(&self) -> anyhow::Result<Index> {
        self.storage.get_last_index().await
    }
    async fn get_snapshot_index(&self) -> anyhow::Result<Index> {
        self.storage.get_snapshot_index().await
    }
    async fn append_new_entry(
        &self,
        command: Bytes,
        ack: Option<Ack>,
        term: Term,
    ) -> anyhow::Result<Index> {
        let _token = self.append_token.acquire().await;

        let cur_last_log_index = self.storage.get_last_index().await?;
        let prev_clock = self
            .storage
            .get_entry(cur_last_log_index)
            .await?
            .unwrap()
            .this_clock;
        let new_index = cur_last_log_index + 1;
        let this_clock = Clock {
            term,
            index: new_index,
        };
        let e = Entry {
            prev_clock,
            this_clock,
            command,
        };
        self.insert_entry(e).await?;
        if let Some(x) = ack {
            self.ack_chans.write().await.insert(new_index, x);
        }
        self.append_notify.notify_waiters();
        Ok(new_index)
    }
    async fn try_insert_entry<A: RaftApp>(
        &self,
        entry: Entry,
        sender_id: Id,
        core: Arc<RaftCore<A>>,
    ) -> anyhow::Result<TryInsertResult> {
        let _token = self.append_token.acquire().await;

        let Clock {
            term: _,
            index: prev_index,
        } = entry.prev_clock;
        if let Some(prev_clock) = self
            .storage
            .get_entry(prev_index)
            .await?
            .map(|x| x.this_clock)
        {
            if prev_clock != entry.prev_clock {
                return Ok(TryInsertResult::Rejected);
            }
        } else {
            // If the entry is snapshot then we should insert this entry without consistency checks.
            // Old entries before the new snapshot will be garbage collected.
            let command = entry.command.clone();
            if std::matches!(Command::deserialize(&command), Command::Snapshot { .. }) {
                let Clock {
                    term: _,
                    index: snapshot_index,
                } = entry.this_clock;
                log::warn!(
                    "log is too old. replicated a snapshot (idx={}) from leader",
                    snapshot_index
                );

                // Snapshot resource is not defined with snapshot_index=1.
                if sender_id != core.id && snapshot_index > 1 {
                    if let Err(e) = core.fetch_snapshot(snapshot_index, sender_id.clone()).await {
                        log::error!("could not fetch app snapshot (idx={}) from sender {}", snapshot_index, sender_id);
                        return Err(e);
                    }
                }
                if let Err(e) = self.insert_snapshot(entry).await {
                    log::error!("could not insert snapshot entry (idx={})", snapshot_index);
                    return Err(e);
                }

                self.commit_index
                    .store(snapshot_index - 1, Ordering::SeqCst);
                self.last_applied
                    .store(snapshot_index - 1, Ordering::SeqCst);

                return Ok(TryInsertResult::Inserted);
            } else {
                return Ok(TryInsertResult::Rejected);
            }
        }

        let Clock {
            term: _,
            index: new_index,
        } = entry.this_clock;

        if let Some(old_clock) = self
            .storage
            .get_entry(new_index)
            .await?
            .map(|e| e.this_clock)
        {
            if old_clock == entry.this_clock {
                // If there is a entry with the same term and index
                // then the entry should be the same so skip insertion.
                Ok(TryInsertResult::Skipped)
            } else {
                log::warn!("log conflicted at idx: {}", new_index);

                let old_last_log_index = self.storage.get_last_index().await?;
                for idx in new_index..old_last_log_index {
                    self.ack_chans.write().await.remove(&idx);
                }

                self.insert_entry(entry).await?;
                Ok(TryInsertResult::Inserted)
            }
        } else {
            self.insert_entry(entry).await?;
            Ok(TryInsertResult::Inserted)
        }
    }
    async fn insert_entry(&self, e: Entry) -> anyhow::Result<()> {
        self.storage.insert_entry(e.this_clock.index, e).await?;
        Ok(())
    }
    async fn insert_snapshot(&self, e: Entry) -> anyhow::Result<()> {
        self.storage.insert_snapshot(e.this_clock.index, e).await?;
        Ok(())
    }
    async fn advance_commit_index<A: RaftApp>(
        &self,
        new_agreement: Index,
        core: Arc<RaftCore<A>>,
    ) -> anyhow::Result<()> {
        let _token = self.commit_token.acquire().await;

        let old_agreement = self.commit_index.load(Ordering::SeqCst);
        if !(new_agreement > old_agreement) {
            return Ok(());
        }

        for i in old_agreement + 1..=new_agreement {
            let e = self.storage.get_entry(i).await?.unwrap();
            let term = e.this_clock.term;
            match Command::deserialize(&e.command) {
                Command::ClusterConfiguration { membership } => {
                    // Leader stepdown should happen iff the last membership change doesn't contain the leader.
                    // This code is safe because doing or not doing leadership transfer will not affect anything
                    // (IOW, it is only a hint) but confuse the leadership which only causes instant downtime.
                    let remove_this_node = !membership.contains(&core.id);
                    let is_last_membership_change =
                        i == core.membership_barrier.load(Ordering::SeqCst);
                    let is_leader =
                        std::matches!(*core.election_state.read().await, ElectionState::Leader);
                    if remove_this_node && is_last_membership_change && is_leader {
                        *core.election_state.write().await = ElectionState::Follower;

                        // If leader node steps down choose one of the follower node to
                        // become candidate immediately so the downtime becomes shorter.
                        core.transfer_leadership().await;
                    }
                }
                Command::Noop => {
                    core.commit_safe_term(term);
                }
                _ => {}
            }

            let mut ack_chans = self.ack_chans.write().await;
            if !ack_chans.contains_key(&i) {
                continue;
            }

            let ack = ack_chans.get(&i).unwrap();
            if std::matches!(ack, Ack::OnCommit(_)) {
                if let Ack::OnCommit(tx) = ack_chans.remove(&i).unwrap() {
                    let _ = tx.send(ack::CommitOk);
                }
            }
        }

        log::debug!("commit_index {} -> {}", old_agreement, new_agreement);
        self.commit_index.store(new_agreement, Ordering::SeqCst);
        self.commit_notify.notify_one();
        Ok(())
    }
    async fn advance_last_applied<A: RaftApp>(
        &self,
        raft_core: Arc<RaftCore<A>>,
    ) -> anyhow::Result<()> {
        let (apply_index, apply_entry, command) = {
            let apply_index = self.last_applied.load(Ordering::SeqCst) + 1;
            let mut e = self.storage.get_entry(apply_index).await?.unwrap();
            let command = std::mem::take(&mut e.command);
            (apply_index, e, command)
        };
        let ok = match Command::deserialize(&command) {
            Command::Snapshot { membership } => {
                let tag = self.storage.get_tag(apply_index).await?;
                log::info!("install app snapshot");
                let res = raft_core
                    .app
                    .install_snapshot(tag.as_ref(), apply_index)
                    .await;
                log::info!("install app snapshot (complete)");
                let success = res.is_ok();
                if success {
                    *self.applied_membership.lock().await = membership;
                    true
                } else {
                    false
                }
            }
            Command::Req { core, ref message } => {
                let res = if core {
                    let res = raft_core.process_message(message).await;
                    res.map(|x| (x, MakeSnapshot::None))
                } else {
                    raft_core.app.apply_message(message, apply_index).await
                };
                match res {
                    Ok((msg, make_snapshot)) => {
                        let mut ack_chans = self.ack_chans.write().await;
                        if ack_chans.contains_key(&apply_index) {
                            let ack = ack_chans.get(&apply_index).unwrap();
                            if std::matches!(ack, Ack::OnApply(_)) {
                                if let Ack::OnApply(tx) = ack_chans.remove(&apply_index).unwrap() {
                                    let _ = tx.send(ack::ApplyOk(msg));
                                }
                            }
                        }

                        match make_snapshot {
                            MakeSnapshot::CopySnapshot(new_tag) => {
                                // Replication proceeds if apply_message's succeeded and regardless of the result of snapshot insertion.

                                // Snapshot becomes orphan when this code fails but it is allowed.
                                let ok = self.storage.put_tag(apply_index, new_tag).await.is_ok();
                                // Inserting a snapshot entry with its corresponding snapshot tag/resource is not allowed
                                // because it is assumed that the at least lastest snapshot entry must have a snapshot tag/resource.
                                if ok {
                                    let snapshot_entry = Entry {
                                        command: Command::serialize(&Command::Snapshot {
                                            membership: self
                                                .applied_membership
                                                .lock()
                                                .await
                                                .clone(),
                                        }),
                                        ..apply_entry
                                    };
                                    let delay_sec =
                                        raft_core.tunable.read().await.compaction_delay_sec;
                                    let delay = Duration::from_secs(delay_sec);
                                    log::info!(
                                        "copy snapshot is made and will be inserted in {}s",
                                        delay_sec
                                    );

                                    self.snapshot_queue
                                        .insert(
                                            snapshot::InsertSnapshot { e: snapshot_entry },
                                            delay,
                                        )
                                        .await;
                                }
                            }
                            MakeSnapshot::FoldSnapshot => {
                                tokio::spawn(async move {
                                    let _ = raft_core
                                        .log
                                        .create_fold_snapshot(apply_index, Arc::clone(&raft_core))
                                        .await;
                                });
                            }
                            MakeSnapshot::None => {}
                        }
                        true
                    }
                    Err(e) => {
                        log::error!("log apply error: {} (core={})", e, core);
                        false
                    }
                }
            }
            Command::ClusterConfiguration { membership } => {
                *self.applied_membership.lock().await = membership;
                true
            }
            _ => true,
        };
        if ok {
            self.apply_error_seq.store(0, Ordering::SeqCst);

            log::debug!("last_applied -> {}", apply_index);
            self.last_applied.store(apply_index, Ordering::SeqCst);
            self.apply_notify.notify_one();
        } else {
            // We assume apply_message typically fails due to
            // 1. temporal storage/network error
            // 2. application bug
            // While the former is recoverable the latter isn't which results in
            // emitting error indefinitely until storage capacity is totally consumed.
            // To avoid this adaptive penalty is inserted after each error.
            let n_old = self.apply_error_seq.load(Ordering::SeqCst);
            let wait_ms: u64 = 100 * (1 << n_old);
            log::error!(
                "log apply failed at index={} (n={}). wait for {}ms",
                apply_index,
                n_old + 1,
                wait_ms
            );
            tokio::time::sleep(Duration::from_millis(wait_ms)).await;
            self.apply_error_seq.fetch_add(1, Ordering::SeqCst);
        }
        Ok(())
    }
    async fn create_fold_snapshot<A: RaftApp>(
        &self,
        new_snapshot_index: Index,
        core: Arc<RaftCore<A>>,
    ) -> anyhow::Result<()> {
        assert!(new_snapshot_index <= self.last_applied.load(Ordering::SeqCst));

        let _token = self.compaction_token.acquire().await;

        let cur_snapshot_index = self.storage.get_snapshot_index().await?;

        if new_snapshot_index <= cur_snapshot_index {
            return Ok(());
        }

        log::info!("create new fold snapshot at index {}", new_snapshot_index);
        let cur_snapshot_entry = self.storage.get_entry(cur_snapshot_index).await?.unwrap();
        if let Command::Snapshot { membership } = Command::deserialize(&cur_snapshot_entry.command)
        {
            let mut base_snapshot_index = cur_snapshot_index;
            let mut new_membership = membership;
            let mut commands = BTreeMap::new();
            for i in cur_snapshot_index + 1..=new_snapshot_index {
                let command = self.storage.get_entry(i).await?.unwrap().command;
                commands.insert(i, command);
            }
            let mut app_messages = vec![];
            for (i, command) in &commands {
                match Command::deserialize(&command) {
                    Command::ClusterConfiguration { membership } => {
                        new_membership = membership;
                    }
                    Command::Req {
                        core: false,
                        message,
                    } => {
                        app_messages.push(message);
                    }
                    Command::Snapshot { membership } => {
                        base_snapshot_index = *i;
                        new_membership = membership;
                        app_messages = vec![];
                    }
                    _ => {}
                }
            }
            let base_tag = self.storage.get_tag(base_snapshot_index).await?;
            let new_tag = core
                .app
                .fold_snapshot(base_tag.as_ref(), app_messages)
                .await?;
            self.storage.put_tag(new_snapshot_index, new_tag).await?;
            let new_snapshot = {
                let mut e = self.storage.get_entry(new_snapshot_index).await?.unwrap();
                e.command = Command::serialize(&Command::Snapshot {
                    membership: new_membership,
                });
                e
            };
            let delay = Duration::from_secs(core.tunable.read().await.compaction_delay_sec);
            self.snapshot_queue
                .insert(snapshot::InsertSnapshot { e: new_snapshot }, delay)
                .await;
            Ok(())
        } else {
            unreachable!()
        }
    }
    async fn run_gc<A: RaftApp>(&self, core: Arc<RaftCore<A>>) -> anyhow::Result<()> {
        let r = self.storage.get_snapshot_index().await?;
        log::debug!("gc .. {}", r);

        // Delete old snapshots
        let ls: Vec<Index> = self
            .storage
            .list_tags()
            .await?
            .range(..r)
            .map(|x| *x)
            .collect();
        for i in ls {
            if let Some(tag) = self.storage.get_tag(i).await?.clone() {
                core.app.delete_resource(&tag).await?;
                self.storage.delete_tag(i).await?;
            }
        }
        // Remove entries
        self.storage.delete_before(r).await?;
        // Remove acks
        let ls: Vec<u64> = self
            .ack_chans
            .read()
            .await
            .range(..r)
            .map(|x| *x.0)
            .collect();
        for i in ls {
            self.ack_chans.write().await.remove(&i);
        }
        Ok(())
    }
}

pub type RaftService<A> = proto_compiled::raft_server::RaftServer<server::Server<A>>;

/// Lift `RaftCore` to `Service`.
pub fn make_service<A: RaftApp>(core: Arc<RaftCore<A>>) -> RaftService<A> {
    tokio::spawn(thread::commit::run(Arc::clone(&core)));
    tokio::spawn(thread::compaction::run(Arc::clone(&core)));
    tokio::spawn(thread::election::run(Arc::clone(&core)));
    tokio::spawn(thread::execution::run(Arc::clone(&core)));
    tokio::spawn(thread::query_executor::run(Arc::clone(&core)));
    tokio::spawn(thread::gc::run(Arc::clone(&core)));
    tokio::spawn(thread::snapshot_installer::run(Arc::clone(&core)));
    let server = server::Server { core };
    proto_compiled::raft_server::RaftServer::new(server)
}
