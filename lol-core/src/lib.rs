#![deny(unused_must_use)]
#![cfg_attr(docsrs, feature(doc_cfg))]

//! Raft is a distributed consensus algorithm widely used recently
//! to build distributed applications like etcd.
//! However, while it is even understandable than notorious Paxos algorithm
//! it is still difficult to implement correct and efficient implementation.
//!
//! This library is a Raft implementation based on Tonic, a gRPC library based
//! on Tokio.
//! By exploiting gRPC features like streaming, the inter-node log replication
//! and snapshot copying is very efficient.

use anyhow::anyhow;
use anyhow::Result;
use async_trait::async_trait;
use bytes::Bytes;
use derive_builder::Builder;
use derive_more::{Display, FromStr};
use futures::stream::StreamExt;
use std::collections::{BTreeMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{Mutex, Notify, RwLock, Semaphore};

mod ack;
mod core_message;
#[cfg(feature = "gateway")]
#[cfg_attr(docsrs, doc(cfg(feature = "gateway")))]
/// Gateway to interact with the cluster.
pub mod gateway;
mod membership;
mod query_queue;
mod quorum_join;
mod server;
#[cfg(feature = "simple")]
#[cfg_attr(docsrs, doc(cfg(feature = "simple")))]
/// Simplified `RaftApp`.
pub mod simple;
mod snapshot;
/// The abstraction of the log storage and some implementations.
pub mod storage;
mod thread;
mod thread_drop;

use ack::Ack;
use storage::RaftStorage;
use tonic::transport::Endpoint;

mod proto_compiled {
    tonic::include_proto!("lol_core");
}

/// Available message types for interaction with the cluster.
pub mod api {
    pub use crate::proto_compiled::{
        AddServerRep, AddServerReq, ApplyRep, ApplyReq, ClusterInfoRep, ClusterInfoReq, CommitRep,
        CommitReq, GetConfigRep, GetConfigReq, RemoveServerRep, RemoveServerReq, StatusRep,
        StatusReq, TimeoutNowRep, TimeoutNowReq, TuneConfigRep, TuneConfigReq,
    };
}

pub use crate::proto_compiled::raft_client::RaftClient;

use storage::{Ballot, Entry};

/// Decision to make a new snapshot.
pub enum MakeSnapshot {
    /// No snapshot will be made.
    None,
    /// Copy snapshot will be made.
    CopySnapshot,
    /// Fold snapshot will be made.
    FoldSnapshot,
}

/// The abstraction of user-defined application.
///
/// Note about the error handling:
/// In Raft, the great rule is the same log should result in the same state.
/// This means the application of a log entry should result in the same state
/// and it is not allowed the same entry succeeds in some node and fails in another node.
/// Therefore function that may change the state (e.g. process_write) should not fail
/// if there is any chance that other node succeeds the same entry.
#[async_trait]
pub trait RaftApp: Sync + Send + 'static {
    /// Process read request.
    /// This operation should **not** change the state of the application.
    async fn process_read(&self, request: &[u8]) -> Result<Vec<u8>>;

    /// Process write request.
    /// This may change the state of the application.
    ///
    /// This function may return `MakeSnapshot` to make a new snapshot.
    /// The snapshot entry corresponding to the copy snapshot is not guaranteed to be made
    /// due to possible I/O errors, etc.
    async fn process_write(
        &self,
        request: &[u8],
        entry_index: Index,
    ) -> Result<(Vec<u8>, MakeSnapshot)>;

    /// Special type of process_write but when the entry is a snapshot entry.
    /// Snapshot is None when apply_index is 1 which is the most youngest snapshot.
    async fn install_snapshot(&self, snapshot: Option<Index>) -> Result<()>;

    /// This function is called from the compaction thread.
    /// It should return new snapshot from accumulative computation with the old_snapshot and the subsequent log entries.
    async fn fold_snapshot(
        &self,
        old_snapshot: Option<Index>,
        requests: Vec<&[u8]>,
        snapshot_index: Index,
    ) -> Result<()>;

    /// Make a snapshot resource and return the tag.
    async fn save_snapshot(&self, st: SnapshotStream, snapshot_index: Index) -> Result<()>;

    /// Make a snapshot stream from a snapshot resource bound to the tag.
    async fn open_snapshot(&self, x: Index) -> Result<SnapshotStream>;

    /// Delete a snapshot resource bound to the tag.
    async fn delete_snapshot(&self, x: Index) -> Result<()>;
}

/// The core-level stream type. It is just a stream of byte chunks.
/// The length of each chunk may be different.
pub type SnapshotStream =
    std::pin::Pin<Box<dyn futures::stream::Stream<Item = anyhow::Result<Bytes>> + Send>>;

type Term = u64;
/// Log entry index.
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

pub use tonic::transport::Uri;

#[derive(
    serde::Serialize, serde::Deserialize, Clone, PartialEq, Eq, Hash, Debug, Display, FromStr,
)]
struct Id(#[serde(with = "http_serde::uri")] Uri);
impl Id {
    fn uri(&self) -> &Uri {
        &self.0
    }
}
impl From<Uri> for Id {
    fn from(x: Uri) -> Id {
        Id(x)
    }
}
#[test]
fn test_uri() {
    let a = "https://192.168.1.1:8000";
    let aa: Uri = a.parse().unwrap();
    let b = "https://192.168.1.1:8000/";
    let bb: Uri = b.parse().unwrap();
    // the equality is made by the canonical form.
    assert_eq!(aa, bb);
    // the string repr is made by the canonical form.
    assert_eq!(aa.to_string(), b);
    assert_ne!(a, b);
}

#[test]
fn test_id() {
    let a = "https://192.168.100.200:8080";
    let b: Id = a.parse().unwrap();
    let c = a.to_string();
    assert_eq!(a, c);
    let d = c.parse().unwrap();
    assert_eq!(b, d);
}

#[test]
fn test_id_serde() {
    let a: Id = "http://192.168.1.1:8080".parse().unwrap();
    let b = bincode::serialize(&a).unwrap();
    let c = bincode::deserialize(&b).unwrap();
    assert_eq!(a, c);
}

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

/// Configuration.
#[derive(Builder)]
pub struct Config {
    #[builder(default = "300")]
    /// Compaction will run in this interval.
    /// You can set this to 0 to disable fold snapshot.
    /// This parameter can be updated online.
    /// default: 300
    compaction_interval_sec: u64,
}
impl Config {
    fn compaction_interval(&self) -> Duration {
        Duration::from_secs(self.compaction_interval_sec)
    }
    fn snapshot_insertion_delay(&self) -> Duration {
        // If the fold compaction is disabled, snapshot insertion delays in 10 seconds.
        // I think this is reasonable tolerance delay for most of the applications.
        let minv = Duration::from_secs(10);
        let v = Duration::from_secs(self.compaction_interval_sec / 2);
        std::cmp::max(minv, v)
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
struct RaftCore {
    id: Id,
    app: Box<dyn RaftApp>,
    query_queue: Mutex<query_queue::QueryQueue>,
    log: Log,
    election_state: RwLock<ElectionState>,
    cluster: RwLock<membership::Cluster>,
    config: RwLock<Config>,
    vote_token: Semaphore,
    // Until noop is committed and safe term is incrememted
    // no new entry in the current term is appended to the log.
    safe_term: AtomicU64,
    // Membership should not be appended until commit_index passes this line.
    membership_barrier: AtomicU64,

    failure_detector: RwLock<FailureDetector>,
}
impl RaftCore {
    async fn new(
        app: impl RaftApp,
        storage: impl RaftStorage,
        id: Uri,
        config: Config,
    ) -> Arc<Self> {
        let id: Id = id.into();
        let init_cluster = membership::Cluster::empty(id.clone()).await;
        let (membership_index, init_membership) =
            Self::find_last_membership(&storage).await.unwrap();
        let init_log = Log::new(storage).await;
        let fd = FailureDetector::watch(id.clone());
        let r = Arc::new(Self {
            app: Box::new(app),
            query_queue: Mutex::new(query_queue::QueryQueue::new()),
            id,
            log: init_log,
            election_state: RwLock::new(ElectionState::Follower),
            cluster: RwLock::new(init_cluster),
            config: RwLock::new(config),
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
    async fn find_last_membership<S: RaftStorage>(storage: &S) -> Result<(Index, HashSet<Id>)> {
        let last = storage.get_last_index().await?;
        let mut ret = (0, HashSet::new());
        for i in (1..=last).rev() {
            let e = storage.get_entry(i).await?.unwrap();
            match Command::deserialize(&e.command) {
                Command::Snapshot { membership } => {
                    ret = (i, membership);
                    break;
                }
                Command::ClusterConfiguration { membership } => {
                    ret = (i, membership);
                    break;
                }
                _ => {}
            }
        }
        Ok(ret)
    }
    async fn init_cluster(self: &Arc<Self>) -> Result<()> {
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
    ) -> Result<()> {
        log::info!("change membership to {:?}", membership);
        self.cluster
            .write()
            .await
            .set_membership(&membership, Arc::clone(&self))
            .await?;
        self.membership_barrier.store(index, Ordering::SeqCst);
        Ok(())
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
    sender_id: Id,
    prev_log_term: Term,
    prev_log_index: Index,
    entries: std::pin::Pin<Box<dyn futures::stream::Stream<Item = LogStreamElem> + Send>>,
}
struct LogStreamElem {
    term: Term,
    index: Index,
    command: Bytes,
}

fn into_out_stream(
    x: LogStream,
) -> impl futures::stream::Stream<Item = crate::proto_compiled::AppendEntryReq> {
    use crate::proto_compiled::{append_entry_req::Elem, AppendStreamEntry, AppendStreamHeader};
    let header_stream = vec![Elem::Header(AppendStreamHeader {
        sender_id: x.sender_id.to_string(),
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
impl RaftCore {
    async fn change_membership(self: &Arc<Self>, command: Bytes, index: Index) -> Result<()> {
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
    async fn queue_entry(self: &Arc<Self>, command: Bytes, ack: Option<Ack>) -> Result<()> {
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
    async fn queue_received_entry(self: &Arc<Self>, mut req: LogStream) -> Result<bool> {
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
    async fn prepare_replication_stream(self: &Arc<Self>, l: Index, r: Index) -> Result<LogStream> {
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
        let st = st1.chain(st2);
        Ok(LogStream {
            sender_id: self.id.clone(),
            prev_log_term,
            prev_log_index,
            entries: Box::pin(st),
        })
    }
    async fn advance_replication(self: &Arc<Self>, follower_id: Id) -> Result<bool> {
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
        let cur_snapshot_index = self.log.get_snapshot_index();
        if old_progress.next_index < cur_snapshot_index {
            log::warn!(
                "entry not found at next_index (idx={}) for {}",
                old_progress.next_index,
                follower_id.uri(),
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

        let res: Result<_> = async {
            let endpoint =
                Endpoint::from(follower_id.uri().clone()).connect_timeout(Duration::from_secs(5));
            let mut conn = RaftClient::connect(endpoint).await?;
            let out_stream = into_out_stream(in_stream);
            let res = conn.send_append_entry(out_stream).await?;
            Ok(res)
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
    async fn find_new_agreement(&self) -> Result<Index> {
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
impl RaftCore {
    async fn fetch_snapshot(&self, snapshot_index: Index, to: Id) -> Result<()> {
        let endpoint = Endpoint::from(to.uri().clone()).connect_timeout(Duration::from_secs(5));

        let mut conn = RaftClient::connect(endpoint).await?;
        let req = proto_compiled::GetSnapshotReq {
            index: snapshot_index,
        };
        let res = conn.get_snapshot(req).await?;
        let out_stream = res.into_inner();
        let in_stream = Box::pin(snapshot::into_in_stream(out_stream));
        self.app.save_snapshot(in_stream, snapshot_index).await?;
        Ok(())
    }
    async fn make_snapshot_stream(&self, snapshot_index: Index) -> Result<Option<SnapshotStream>> {
        let st = self.app.open_snapshot(snapshot_index).await?;
        Ok(Some(st))
    }
}
// Election
impl RaftCore {
    async fn save_ballot(&self, v: Ballot) -> Result<()> {
        self.log.storage.save_ballot(v).await
    }
    async fn load_ballot(&self) -> Result<Ballot> {
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
    ) -> Result<bool> {
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
        log::info!(
            "voted response to {} = grant: {}",
            candidate_id.uri(),
            grant
        );
        Ok(grant)
    }
    async fn request_votes(
        self: &Arc<Self>,
        aim_term: Term,
        force_vote: bool,
        pre_vote: bool,
    ) -> Result<bool> {
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
                    candidate_id: myid.to_string(),
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
                let res: Result<_> = async {
                    let endpoint =
                        Endpoint::from(endpoint.uri().clone()).timeout(Duration::from_secs(5));
                    let mut conn = RaftClient::connect(endpoint).await?;
                    let res = conn.request_vote(req).await?;
                    Ok(res)
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
    async fn after_votes(self: &Arc<Self>, aim_term: Term, ok: bool) -> Result<()> {
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
    async fn try_promote(self: &Arc<Self>, force_vote: bool) -> Result<()> {
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
    async fn send_heartbeat(&self, follower_id: Id) -> Result<()> {
        let endpoint = Endpoint::from(follower_id.uri().clone()).timeout(Duration::from_secs(5));
        let req = {
            let term = self.load_ballot().await?.cur_term;
            proto_compiled::HeartbeatReq {
                term,
                leader_id: self.id.to_string(),
                leader_commit: self.log.commit_index.load(Ordering::SeqCst),
            }
        };
        if let Ok(mut conn) = RaftClient::connect(endpoint).await {
            let res = conn.send_heartbeat(req).await;
            if res.is_err() {
                log::warn!("heartbeat to {} failed", follower_id.uri());
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
    ) -> Result<()> {
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
            log::info!("learn the current leader ({})", leader_id.uri());
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
            let endpoint = Endpoint::from(id.uri().clone()).timeout(Duration::from_secs(5));
            if let Ok(mut conn) = RaftClient::connect(endpoint).await {
                let req = proto_compiled::TimeoutNowReq {};
                let _ = conn.timeout_now(req).await;
            }
        });
    }
}
struct Log {
    storage: Box<dyn RaftStorage>,
    ack_chans: RwLock<BTreeMap<Index, Ack>>,

    snapshot_index: AtomicU64, // Monotonic
    last_applied: AtomicU64,   // Monotonic
    commit_index: AtomicU64,   // Monotonic

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
    async fn new(storage: impl RaftStorage) -> Self {
        let snapshot_index = match storage::find_last_snapshot_index(&storage)
            .await
            .expect("failed to find initial snapshot index")
        {
            Some(x) => x,
            None => 0,
        };
        // When the storage is persistent initial commit_index and last_applied
        // should be set appropriately just before the snapshot index.
        let start_index = if snapshot_index == 0 {
            0
        } else {
            snapshot_index - 1
        };
        Self {
            storage: Box::new(storage),
            ack_chans: RwLock::new(BTreeMap::new()),

            snapshot_index: snapshot_index.into(),
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
    async fn get_last_log_index(&self) -> Result<Index> {
        self.storage.get_last_index().await
    }
    fn get_snapshot_index(&self) -> Index {
        self.snapshot_index.load(Ordering::SeqCst)
    }
    async fn append_new_entry(
        &self,
        command: Bytes,
        ack: Option<Ack>,
        term: Term,
    ) -> Result<Index> {
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
    async fn try_insert_entry(
        &self,
        entry: Entry,
        sender_id: Id,
        core: Arc<RaftCore>,
    ) -> Result<TryInsertResult> {
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
                        log::error!(
                            "could not fetch app snapshot (idx={}) from sender {}",
                            snapshot_index,
                            sender_id.uri(),
                        );
                        return Err(e);
                    }
                }
                let inserted = self
                    .snapshot_queue
                    .insert(entry, Duration::from_millis(0))
                    .await;
                if !inserted.await {
                    anyhow::bail!("failed to insert snapshot entry (idx={})", snapshot_index);
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
    async fn insert_entry(&self, e: Entry) -> Result<()> {
        self.storage.insert_entry(e.this_clock.index, e).await?;
        Ok(())
    }
    async fn insert_snapshot(&self, e: Entry) -> Result<()> {
        let new_snapshot_index = e.this_clock.index;
        self.storage.insert_entry(e.this_clock.index, e).await?;
        self.snapshot_index
            .fetch_max(new_snapshot_index, Ordering::SeqCst);
        Ok(())
    }
    async fn advance_commit_index(&self, new_agreement: Index, core: Arc<RaftCore>) -> Result<()> {
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
    async fn advance_last_applied(&self, raft_core: Arc<RaftCore>) -> Result<()> {
        let (apply_index, apply_entry, command) = {
            let apply_index = self.last_applied.load(Ordering::SeqCst) + 1;
            let mut e = self.storage.get_entry(apply_index).await?.unwrap();
            let command = std::mem::take(&mut e.command);
            (apply_index, e, command)
        };
        let ok = match Command::deserialize(&command) {
            Command::Snapshot { membership } => {
                log::info!("install app snapshot");
                let snapshot = if apply_index == 1 {
                    None
                } else {
                    Some(apply_index)
                };
                let res = raft_core.app.install_snapshot(snapshot).await;
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
                assert_eq!(core, false);
                let res = raft_core.app.process_write(message, apply_index).await;
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
                            MakeSnapshot::CopySnapshot => {
                                // Now that RaftApp's already made a snapshot resource then we will commit the snapshot entry.
                                let snapshot_entry = Entry {
                                    command: Command::serialize(&Command::Snapshot {
                                        membership: self.applied_membership.lock().await.clone(),
                                    }),
                                    ..apply_entry
                                };
                                let delay =
                                    raft_core.config.read().await.snapshot_insertion_delay();
                                log::info!(
                                    "copy snapshot is made and will be inserted in {:?}",
                                    delay
                                );

                                let _ = self.snapshot_queue.insert(snapshot_entry, delay).await;
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
            // We assume process_write typically fails due to
            // some recoverable temporary network/storage errors.
            // It should return ok to skip the broken entry otherwise.
            // Retry will be done with adaptive penalty in-between
            // so these error will be more likely to recover.
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
    async fn create_fold_snapshot(
        &self,
        new_snapshot_index: Index,
        core: Arc<RaftCore>,
    ) -> Result<()> {
        assert!(new_snapshot_index <= self.last_applied.load(Ordering::SeqCst));

        let _token = self.compaction_token.acquire().await;

        let cur_snapshot_index = self.get_snapshot_index();

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
            let base_snapshot = if base_snapshot_index == 1 {
                None
            } else {
                Some(base_snapshot_index)
            };
            core.app
                .fold_snapshot(base_snapshot, app_messages, new_snapshot_index)
                .await?;
            let new_snapshot = {
                let mut e = self.storage.get_entry(new_snapshot_index).await?.unwrap();
                e.command = Command::serialize(&Command::Snapshot {
                    membership: new_membership,
                });
                e
            };
            let delay = core.config.read().await.snapshot_insertion_delay();
            let _ = self.snapshot_queue.insert(new_snapshot, delay).await;
            Ok(())
        } else {
            unreachable!()
        }
    }
    async fn run_gc(&self, core: Arc<RaftCore>) -> Result<()> {
        let l = self.storage.get_head_index().await?;
        let r = self.get_snapshot_index();
        log::debug!("gc {}..{}", l, r);

        for i in l..r {
            // Remove remaining ack?
            // Not sure it really exists.
            self.ack_chans.write().await.remove(&i);

            let entry = self.storage.get_entry(i).await?;
            let entry = entry.unwrap();
            match Command::deserialize(&entry.command) {
                Command::Snapshot { .. } => {
                    // Delete snapshot entry.
                    // There should be snapshot resource to this snapshot entry.
                    core.app.delete_snapshot(i).await?;
                }
                _ => {}
            }
            // Delete the entry.
            // but after everything is successfully deleted.
            self.storage.delete_entry(i).await?;
        }

        Ok(())
    }
}

/// A Raft implementation of `tower::Service`.
pub type RaftService = proto_compiled::raft_server::RaftServer<server::Server>;

/// Make a `RaftService`.
pub async fn make_raft_service(
    app: impl RaftApp,
    storage: impl RaftStorage,
    id: Uri,
    config: Config,
) -> RaftService {
    let core = RaftCore::new(app, storage, id, config).await;

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
