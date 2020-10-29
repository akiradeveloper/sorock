#![deny(unused_must_use)]

#![cfg_attr(docsrs, feature(doc_cfg))]

use anyhow::anyhow;
use async_trait::async_trait;
use std::collections::{HashMap, BTreeMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{Mutex, RwLock, Semaphore};
use bytes::Bytes;

/// simple and backward-compatible RaftApp trait.
pub mod compat;
/// the abstraction for the backing storage and some implementations.
pub mod storage;
mod ack;
/// utilities to connect nodes and cluster.
pub mod connection;
/// the request and response that RaftCore talks.
pub mod core_message;
mod membership;
mod query_queue;
mod quorum_join;
mod thread;
mod thread_drop;
/// the snapshot abstraction and some basic implementations.
pub mod snapshot;

use ack::Ack;
use connection::{Endpoint, EndpointConfig};
use thread::notification::Notification;
use storage::RaftStorage;
use snapshot::SnapshotTag;

// this is currently fixed but can be place in tunable if it is needed.
const ELECTION_TIMEOUT_MS: u64 = 500;

/// proto file compiled.
pub mod proto_compiled {
    tonic::include_proto!("lol_core");
}

use storage::{Entry, Vote};

/// the abstraction for user-defined application runs on the RaftCore.
#[async_trait]
pub trait RaftApp: Sync + Send + 'static {
    /// how state machine interacts with inputs from clients.
    async fn process_message(&self, request: &[u8]) -> anyhow::Result<Vec<u8>>;
    /// almost same as process_message but is called in log application path.
    /// this function may return new "copy snapshot" as a copy of the state after application.
    async fn apply_message(&self, request: &[u8], apply_index: Index) -> anyhow::Result<(Vec<u8>, Option<SnapshotTag>)>;
    /// special type of apply_message but when the entry is snapshot entry.
    /// snapshot is None happens iff apply_index is 1 which is the most initial snapshot.
    async fn install_snapshot(&self, snapshot: Option<&SnapshotTag>, apply_index: Index) -> anyhow::Result<()>;
    /// this function is called from compaction threads.
    /// it should return new snapshot from accumulative compution with the old_snapshot and the subsequent log entries.
    async fn fold_snapshot(
        &self,
        old_snapshot: Option<&SnapshotTag>,
        requests: Vec<&[u8]>,
    ) -> anyhow::Result<SnapshotTag>;
    /// make a snapshot resource and returns the tag.
    async fn from_snapshot_stream(&self, st: snapshot::SnapshotStream, snapshot_index: Index) -> anyhow::Result<SnapshotTag>;
    /// make a snapshot stream from a snapshot resource bound to the tag.
    async fn to_snapshot_stream(&self, x: &SnapshotTag) -> snapshot::SnapshotStream;
    /// delete a snapshot resource bound to the tag.
    async fn delete_resource(&self, x: &SnapshotTag) -> anyhow::Result<()>;
}

type Term = u64;
/// log index.
pub type Index = u64;
type Clock = (Term, Index);
/// each node is identified by a pair of ip and port.
pub type Id = String;

#[derive(serde::Serialize, serde::Deserialize)]
enum CommandB<'a> {
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
        message: &'a [u8]
    }
}
impl <'a> CommandB<'a> {
    fn serialize(x: &CommandB) -> Vec<u8> {
        rmp_serde::to_vec(x).unwrap()
    }
    fn deserialize(x: &[u8]) -> CommandB {
        rmp_serde::from_slice(x).unwrap()
    }
}
#[derive(Clone, Debug)]
enum Command {
    Noop,
    Snapshot {
        membership: HashSet<Id>,
    },
    ClusterConfiguration {
        membership: HashSet<Id>,
    },
    Req {
        core: bool,
        message: Bytes
    }
}
impl From<Bytes> for Command {
    fn from(x: Bytes) -> Self {
        let y = CommandB::deserialize(x.as_ref());
        match y {
            CommandB::Noop => Command::Noop,
            CommandB::Snapshot { membership } => Command::Snapshot { membership },
            CommandB::ClusterConfiguration { membership } => Command::ClusterConfiguration { membership },
            CommandB::Req { core, ref message } => Command::Req { core, message: Bytes::copy_from_slice(message) },
        }
    }
}
impl Into<Bytes> for Command {
    fn into(self) -> Bytes {
        let y = match self {
            Command::Noop => CommandB::Noop,
            Command::Snapshot { membership } => CommandB::Snapshot { membership },
            Command::ClusterConfiguration { membership } => CommandB::ClusterConfiguration { membership },
            Command::Req { core, ref message } => CommandB::Req { core, message: &message }
        };
        CommandB::serialize(&y).into()
    }
}
#[derive(Clone, Copy)]
enum ElectionState {
    Leader,
    Candidate,
    Follower,
}
/// static configuration in initialization.
pub struct Config {
    pub id: Id,
}
/// dynamic configurations.
pub struct TunableConfig {
    /// snapshot will be inserted into log after this delay.
    pub compaction_delay_sec: u64,
    /// the interval that compaction runs.
    /// you can set this to 0 and fold snapshot will never be created.
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
/// RaftCore is the heart of the Raft system.
/// it does everything Raft should do like election, dynamic membership change,
/// log replication, sending snapshot in stream and interaction with user-defined RaftApp.
pub struct RaftCore<A: RaftApp> {
    id: Id,
    app: A,
    query_queue: Mutex<query_queue::QueryQueue>,
    last_heartbeat_received: Mutex<Instant>,
    log: Log,
    election_state: RwLock<ElectionState>,
    cluster: RwLock<membership::Cluster>,
    tunable: RwLock<TunableConfig>,
    election_token: Semaphore,
    safe_term: AtomicU64,
}
impl<A: RaftApp> RaftCore<A> {
    pub async fn new<S: RaftStorage>(app: A, storage: S, config: Config, tunable: TunableConfig) -> Arc<Self> {
        let id = config.id;
        let init_cluster = membership::Cluster::empty(id.clone()).await;
        let init_membership = Self::find_last_membership(&storage).await.unwrap();
        let init_log = Log::new(Box::new(storage)).await;
        let r = Arc::new(Self {
            app,
            query_queue: Mutex::new(query_queue::QueryQueue::new()),
            id,
            last_heartbeat_received: Mutex::new(Instant::now()),
            log: init_log,
            election_state: RwLock::new(ElectionState::Follower),
            cluster: RwLock::new(init_cluster),
            tunable: RwLock::new(tunable),
            election_token: Semaphore::new(1),
            safe_term: 0.into(),
        });
        log::info!("initial membership is {:?}", init_membership);
        r.cluster.write().await.set_membership(&init_membership, Arc::clone(&r)).await.unwrap();
        r
    }
    async fn find_last_membership<S: RaftStorage>(storage: &S) -> anyhow::Result<HashSet<Id>> {
        let from = storage.get_snapshot_index().await?;
        if from == 0 {
            return Ok(HashSet::new())
        }
        let to = storage.get_last_index().await?;
        assert!(from <= to);
        let mut ret = HashSet::new();
        for i in from ..= to {
            let e = storage.get_entry(i).await?.unwrap();
            match e.command.into() {
                Command::Snapshot { membership } => {
                    ret = membership;
                },
                Command::ClusterConfiguration { membership } => {
                    ret = membership;
                },
                _ => {}
            }
        }
        Ok(ret)
    }
    async fn init_cluster(self: &Arc<Self>) -> anyhow::Result<()> {
        let snapshot = Entry {
            prev_clock: (0, 0),
            this_clock: (0, 1),
            command: Command::Snapshot {
                membership: HashSet::new(),
            }.into(),
        };
        self.log.insert_snapshot(snapshot).await?;
        let mut membership = HashSet::new();
        membership.insert(self.id.clone());
        let add_server = Entry {
            prev_clock: (0, 1),
            this_clock: (0, 2),
            command: Command::ClusterConfiguration {
                membership: membership.clone(),
            }.into(),
        };
        self.log.insert_entry(add_server).await?;
        self.set_membership(&membership).await?;

        self.log.advance_commit_index(2, Arc::clone(&self)).await?;
        Ok(())
    }
    async fn set_membership(self: &Arc<Self>, membership: &HashSet<Id>) -> anyhow::Result<()> {
        log::info!("change membership to {:?}", membership);
        self.cluster.write().await.set_membership(&membership, Arc::clone(&self)).await?;
        Ok(())
    }
    async fn process_message(self: &Arc<Self>, msg: &[u8]) -> anyhow::Result<Vec<u8>> {
        let req = core_message::Req::deserialize(msg).unwrap();
        match req {
            core_message::Req::InitCluster => {
                let res = if self.cluster.read().await.internal.len() == 0 {
                    log::info!("init cluster");
                    self.init_cluster().await?;
                    core_message::Rep::InitCluster { ok: true }
                } else {
                    core_message::Rep::InitCluster { ok: false }
                };
                Ok(core_message::Rep::serialize(&res))
            }
            core_message::Req::ClusterInfo => {
                let res = core_message::Rep::ClusterInfo {
                    leader_id: self.load_vote().await?.voted_for,
                    membership: {
                        let mut xs: Vec<_> =
                            self.cluster.read().await.get_membership().into_iter().collect();
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
        // in case last_applied == commit_index and there is no subsequent entries after this line,
        // no notification on last_applied's change will be made and this query will never be processed.
        // to avoid this, here manually kicks the execution of query_queue.
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
struct AppendEntryElem {
    term: Term,
    index: Index,
    command: Bytes,
}
struct AppendEntryBuffer {
    sender_id: String,
    prev_log_term: Term,
    prev_log_index: Index,
    entries: Vec<AppendEntryElem>,
}
fn into_stream(
    req: AppendEntryBuffer,
) -> impl futures::stream::Stream<Item = crate::proto_compiled::AppendEntryReq> {
    use crate::proto_compiled::{append_entry_req::Elem, AppendStreamHeader, AppendStreamEntry};
    let mut elems = vec![];

    elems.push(Elem::Header(AppendStreamHeader {
        sender_id: req.sender_id,
        prev_log_index: req.prev_log_index,
        prev_log_term: req.prev_log_term,
    }));

    for e in &req.entries {
        elems.push(Elem::Entry(AppendStreamEntry {
            term: e.term,
            index: e.index,
            command: e.command.as_ref().into(),
        }));
    }
    futures::stream::iter(
        elems
            .into_iter()
            .map(|x| crate::proto_compiled::AppendEntryReq { elem: Some(x) }),
    )
}
// replication
impl<A: RaftApp> RaftCore<A> {
    async fn change_membership(self: &Arc<Self>, command: Command) -> anyhow::Result<()> {
        match command {
            Command::Snapshot { membership } => {
                self.set_membership(&membership).await?
            },
            Command::ClusterConfiguration { membership } => {
                self.set_membership(&membership).await?
            },
            _ => {},
        }
        Ok(())
    }
    fn commit_safe_term(&self, term: Term) {
        log::info!("noop entry of term {} is successfully committed", term);
        self.safe_term.fetch_max(term, Ordering::SeqCst);
    }
    // leader calls this fucntion to append new entry to its log.
    async fn queue_entry(self: &Arc<Self>, command: Command, ack: Option<Ack>) -> anyhow::Result<()> {
        let term = self.load_vote().await?.cur_term;
        // safe term is a term that noop entry is successfully committed.
        if self.safe_term.load(Ordering::SeqCst) < term {
            return Err(anyhow!("noop entry for term {} isn't committed yet."));
        }
        // command.clone() is cheap because the message buffer is Bytes.
        self.log.append_new_entry(command.clone(), ack, term).await?;
        // change membership when cluster configuration is appended.
        self.change_membership(command).await?;
        Ok(())
    }
    // follower calls this function when it receives entries from the leader.
    async fn queue_received_entry(self: &Arc<Self>, req: AppendEntryBuffer) -> anyhow::Result<bool> {
        let mut prev_clock = (req.prev_log_term, req.prev_log_index);
        for e in req.entries {
            let entry = Entry {
                prev_clock,
                this_clock: (e.term, e.index),
                command: e.command,
            };
            let command = entry.command.clone();
            if !self.log.try_insert_entry(entry, req.sender_id.clone(), Arc::clone(&self)).await? {
                log::warn!("rejected append entry (clock={:?})", (e.term, e.index));
                return Ok(false);
            }
            self.change_membership(command.into()).await?;

            prev_clock = (e.term, e.index);
        }
        Ok(true)
    }
    async fn prepare_replication_request(
        &self,
        l: Index,
        r: Index,
    ) -> anyhow::Result<AppendEntryBuffer> {
        let head = self.log.storage.get_entry(l).await?.unwrap();
        let (prev_log_term, prev_log_index) = head.prev_clock;
        let (term, index) = head.this_clock;
        let e = AppendEntryElem {
            term,
            index,
            command: head.command,
        };
        let mut req = AppendEntryBuffer {
            sender_id: self.id.clone(),
            prev_log_term,
            prev_log_index,
            entries: vec![e],
        };

        for idx in l + 1..r {
            let e = {
                let x = self.log.storage.get_entry(idx).await?.unwrap();
                let (term, index) = x.this_clock;
                AppendEntryElem {
                    term,
                    index,
                    command: x.command,
                }
            };
            req.entries.push(e);
        }

        Ok(req)
    }
    async fn advance_replication(&self, follower_id: Id) -> anyhow::Result<bool> {
        let member = self
            .cluster
            .read()
            .await
            .internal
            .get(&follower_id)
            .unwrap()
            .clone();

        let old_progress = member.progress.unwrap();
        let cur_last_log_index = self.log.get_last_log_index().await?;

        // more entries to send?
        let should_send = cur_last_log_index >= old_progress.next_index;
        if !should_send {
            return Ok(false);
        }

        // the entries to send could be deleted due to previous compactions.
        // in this case, replication will reset from the current snapshot index.
        let cur_snapshot_index = self.log.get_snapshot_index().await?;
        if old_progress.next_index < cur_snapshot_index {
            log::warn!(
                "entry not found at next_index (idx={}) for {}",
                old_progress.next_index,
                follower_id
            );
            let mut cluster = self.cluster.write().await;
            let new_progress = membership::ReplicationProgress::new(cur_snapshot_index);
            cluster.internal.get_mut(&follower_id).unwrap().progress = Some(new_progress);
            return Ok(true);
        }

        let n_max_possible = cur_last_log_index - old_progress.next_index + 1;
        let n = std::cmp::min(old_progress.next_max_cnt, n_max_possible);
        assert!(n >= 1);

        let req = self
            .prepare_replication_request(old_progress.next_index, old_progress.next_index + n)
            .await?;

        let res = async {
            let endpoint = member.endpoint;
            let mut conn = endpoint.connect().await?;
            conn.send_append_entry(into_stream(req)).await
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
            cluster.internal.get_mut(&follower_id).unwrap().progress = Some(new_progress);
        }
        if incremented {
            self.log.replication_notification.lock().await.publish();
        }

        Ok(true)
    }
    async fn find_new_agreement(&self) -> anyhow::Result<Index> {
        let cluster = self.cluster.read().await.internal.clone();
        let new_agreement = {
            let n = cluster.len();
            let mid = n / 2;
            let mut match_indices = vec![];
            for (id, member) in cluster {
                if id == self.id {
                    let last_log_index = self.log.get_last_log_index().await?;
                    match_indices.push(last_log_index);
                } else {
                    match_indices.push(member.progress.unwrap().match_index);
                }
            }
            match_indices.sort();
            match_indices.reverse();
            match_indices[mid]
        };
        Ok(new_agreement)
    }
}
// snapshot
impl<A: RaftApp> RaftCore<A> {
    async fn fetch_snapshot(&self, snapshot_index: Index, to: Id) -> anyhow::Result<()> {
        let config = EndpointConfig::default().timeout(Duration::from_secs(5));
        let mut conn = Endpoint::new(to).connect_with(config).await?;
        let req = proto_compiled::GetSnapshotReq {
            index: snapshot_index,
        };
        let res = conn.get_snapshot(req).await?;
        let st = res.into_inner();
        let st = Box::pin(snapshot::map_in(st));
        let tag = self.app.from_snapshot_stream(st, snapshot_index).await?;
        self.log.storage.put_tag(snapshot_index, tag).await?;
        Ok(())
    }
    async fn make_snapshot_stream(&self, snapshot_index: Index) -> anyhow::Result<Option<snapshot::SnapshotStream>> {
        let tag = self.log.storage.get_tag(snapshot_index).await?;
        if tag.is_none() {
            return Ok(None)
        }
        let tag = tag.unwrap();
        let st = self.app.to_snapshot_stream(&tag).await;
        Ok(Some(st))
    }
}
// election
impl<A: RaftApp> RaftCore<A> {
    async fn store_vote(&self, v: Vote) -> anyhow::Result<()> {
        self.log.storage.store_vote(v).await
    }
    async fn load_vote(&self) -> anyhow::Result<Vote> {
        self.log.storage.load_vote().await
    }
    async fn receive_vote(
        &self,
        candidate_term: Term,
        candidate_id: Id,
        candidate_last_log_clock: Clock,
        force_vote: bool,
    ) -> anyhow::Result<bool> {
        if !force_vote {
            let elapsed = Instant::now() - *self.last_heartbeat_received.lock().await;
            if elapsed < Duration::from_millis(ELECTION_TIMEOUT_MS) {
                return Ok(false)
            }
        }

        let mut vote = self.load_vote().await?;
        if candidate_term < vote.cur_term {
            log::warn!("candidate term is older. reject vote");
            return Ok(false);
        }

        if candidate_term > vote.cur_term {
            log::warn!("received newer term. reset vote");
            vote.cur_term = candidate_term;
            vote.voted_for = None;
            *self.election_state.write().await = ElectionState::Follower;
        }

        let cur_last_index = self.log.get_last_log_index().await?;

        // suppose we have 3 in-memory nodes ND0-2 and initially ND0 is the leader,
        // log is fully replicated between nodes and there is no in-coming entries.
        // suddenly, ND0 and 1 is crashed and soon later rebooted.
        // in this case, ND2 should become leader by getting vote from either ND0 or ND1.
        // this is why using weakest clock (0,0) here when there is no entry in the log.
        let this_last_log_clock = self
            .log
            .storage
            .get_entry(cur_last_index)
            .await?.map(|x| x.this_clock).unwrap_or((0,0));

        if candidate_last_log_clock < this_last_log_clock {
            log::warn!("candidate clock is older. reject vote");
            self.store_vote(vote).await?;
            return Ok(false);
        }

        let grant = match &vote.voted_for {
            None => {
                vote.voted_for = Some(candidate_id.clone());
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

        self.store_vote(vote).await?;
        log::info!("voted response to {} = grant: {}", candidate_id, grant);
        Ok(grant)
    }
    async fn request_votes(self: &Arc<Self>, aim_term: Term, force_vote: bool) -> anyhow::Result<bool> {
        let (others, quorum) = {
            let cur_cluster = self.cluster.read().await.internal.clone();
            let n = cur_cluster.len();
            let majority = (n / 2) + 1;
            let include_self = cur_cluster.contains_key(&self.id);
            let mut others = vec![];
            for (id, member) in cur_cluster {
                if id != self.id {
                    others.push(member.endpoint);
                }
            }
            // -1 = self vote
            let quorum = if include_self { majority - 1 } else { majority };
            (others, quorum)
        };

        let last_log_index = self.log.get_last_log_index().await?;
        let last_log_clock = self
            .log
            .storage
            .get_entry(last_log_index)
            .await?
            .unwrap()
            .this_clock;

        let timeout = Duration::from_secs(5);
        let mut vote_requests = vec![];
        for endpoint in others {
            let myid = self.id.clone();
            vote_requests.push(async move {
                let (last_log_term, last_log_index) = last_log_clock;
                let req = crate::proto_compiled::RequestVoteReq {
                    term: aim_term,
                    candidate_id: myid,
                    last_log_term,
                    last_log_index,
                    // $4.2.3
                    // if force_vote is set, the receiver server accepts the vote request
                    // regardless of the heartbeat timeout otherwise the vote request is
                    // dropped when it's receiving heartbeat.
                    force_vote,
                };
                let config = EndpointConfig::default().timeout(timeout);
                let res = async {
                    let mut conn = endpoint.connect_with(config).await?;
                    conn.request_vote(req).await
                }
                .await;
                match res {
                    Ok(res) => res.into_inner().vote_granted,
                    Err(_) => false,
                }
            });
        }
        let ok = quorum_join::quorum_join(timeout, quorum, vote_requests).await;
        Ok(ok)
    }
    async fn after_votes(self: &Arc<Self>, aim_term: Term, ok: bool) -> anyhow::Result<()> {
        if ok {
            log::info!("got enough votes from the cluster. promoted to leader");

            // as soon as the node becomes the leader, replicate noop entries with term.
            self.log.append_new_entry(Command::Noop, None, aim_term).await?;

            // initialize replication progress
            {
                let initial_progress = membership::ReplicationProgress::new(
                    self.log.get_last_log_index().await?,
                );
                let mut cluster = self.cluster.write().await;
                for (id, member) in &mut cluster.internal {
                    if id != &self.id {
                        member.progress = Some(initial_progress.clone());
                    }
                }
            }

            *self.election_state.write().await = ElectionState::Leader;

            let _ = self.broadcast_heartbeat().await;
        } else {
            log::info!("failed to become leader. now back to follower");
            *self.election_state.write().await = ElectionState::Follower;
        }
        Ok(())
    }
    async fn try_promote(self: &Arc<Self>, force_vote: bool) -> anyhow::Result<()> {
        let _token = self.election_token.acquire().await;

        // vote to self
        let aim_term = {
            let mut new_vote = self.load_vote().await?;
            let cur_term = new_vote.cur_term;
            let aim_term = cur_term + 1;
            new_vote.cur_term = aim_term;
            new_vote.voted_for = Some(self.id.clone());

            self.store_vote(new_vote).await?;
            *self.election_state.write().await = ElectionState::Candidate;
            aim_term
        };

        log::info!("start election. try promote at term {}", aim_term);

        // try to promote at the term.
        // failing some I/O operations during election will be considered as election failure.
        let ok = self.request_votes(aim_term, force_vote).await.unwrap_or(false);
        self.after_votes(aim_term, ok).await?;

        Ok(())
    }
    async fn broadcast_heartbeat(&self) -> anyhow::Result<()> {
        let cluster = self.cluster.read().await.internal.clone();
        let mut futs = vec![];
        for (id, member) in cluster {
            if id == self.id {
                continue;
            }
            let endpoint = member.endpoint;
            let config = EndpointConfig::default().timeout(Duration::from_millis(100));
            let req = {
                let term = self.load_vote().await?.cur_term;
                proto_compiled::HeartbeatReq {
                    term,
                    leader_id: self.id.clone(),
                    leader_commit: self.log.commit_index.load(Ordering::SeqCst),
                }
            };
            futs.push(async move {
                if let Ok(mut conn) = endpoint.connect_with(config).await {
                    let res = conn.send_heartbeat(req).await;
                    if res.is_err() {
                        log::warn!("heartbeat to {} failed", id);
                    }
                }
            })
        }
        futures::future::join_all(futs).await;
        Ok(())
    }
    async fn receive_heartbeat(
        self: &Arc<Self>,
        leader_id: Id,
        leader_term: Term,
        leader_commit: Index,
    ) -> anyhow::Result<()> {
        let mut vote = self.load_vote().await?;
        if leader_term < vote.cur_term {
            log::warn!("heartbeat is stale. rejected");
            return Ok(());
        }

        // now the heartbeat is valid and record the time
        *self.last_heartbeat_received.lock().await = Instant::now();

        if leader_term > vote.cur_term {
            log::warn!("received heartbeat with newer term. reset vote");
            vote.cur_term = leader_term;
            vote.voted_for = None;
            *self.election_state.write().await = ElectionState::Follower;
        }

        if vote.voted_for != Some(leader_id.clone()) {
            log::info!("learn the current leader ({})", leader_id);
            vote.voted_for = Some(leader_id);
        }

        self.store_vote(vote).await?;

        let new_commit_index = std::cmp::min(
            leader_commit,
            self.log.get_last_log_index().await?,
        );
        self.log
            .advance_commit_index(new_commit_index, Arc::clone(&self))
            .await?;

        Ok(()) 
    }
    async fn transfer_leadership(&self) {
        let mut xs = vec![];
        let cluster = self.cluster.read().await.internal.clone();
        for (id, member) in cluster {
            if id != self.id {
                let prog = member.progress.unwrap();
                xs.push((prog.match_index, member));
            }
        }
        xs.sort_by_key(|x| x.0);

        // choose the one with the higher match_index as the next leader.
        if let Some((_, member)) = xs.pop() {
            tokio::spawn(async move {
                if let Ok(mut conn) = member.endpoint.connect().await {
                    let req = proto_compiled::TimeoutNowReq {};
                    let _ = conn.timeout_now(req).await;
                }
            });
        }
    }
}
struct Log {
    storage: Box<dyn RaftStorage>,
    ack_chans: RwLock<BTreeMap<Index, Ack>>,

    last_applied: AtomicU64,   // monotonic
    commit_index: AtomicU64,   // monotonic

    append_token: Semaphore,
    commit_token: Semaphore,
    compaction_token: Semaphore,

    append_notification: Mutex<Notification>,
    replication_notification: Mutex<Notification>,
    commit_notification: Mutex<Notification>,
    apply_notification: Mutex<Notification>,

    applied_membership: Mutex<HashSet<Id>>,
    snapshot_queue: snapshot::SnapshotQueue,
}
impl Log {
    async fn new(storage: Box<dyn RaftStorage>) -> Self {
        let snapshot_index = storage.get_snapshot_index().await.unwrap();
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

            append_notification: Mutex::new(Notification::new()),
            replication_notification: Mutex::new(Notification::new()),
            commit_notification: Mutex::new(Notification::new()),
            apply_notification: Mutex::new(Notification::new()),

            applied_membership: Mutex::new(HashSet::new()),
            snapshot_queue: snapshot::SnapshotQueue::new(),
        }
    }
    async fn get_last_log_index(&self) -> anyhow::Result<Index> {
        self.storage.get_last_index().await
    }
    async fn get_snapshot_index(&self) -> anyhow::Result<Index> {
        self.storage.get_snapshot_index().await
    }
    async fn append_new_entry(&self, command: Command, ack: Option<Ack>, term: Term) -> anyhow::Result<Index> {
        let _token = self.append_token.acquire().await;

        let cur_last_log_index = self.storage.get_last_index().await?;
        let prev_clock = self.storage.get_entry(cur_last_log_index).await?.unwrap().this_clock;
        let new_index = cur_last_log_index + 1;
        let this_clock = (term, new_index);
        let e = Entry {
            prev_clock,
            this_clock,
            command: command.into(),
        };
        self.insert_entry(e).await?;
        if let Some(x) = ack {
            self.ack_chans.write().await.insert(new_index, x);
        }
        self.append_notification.lock().await.publish();
        Ok(new_index)
    }
    async fn try_insert_entry<A: RaftApp>(&self, entry: Entry, sender_id: Id, core: Arc<RaftCore<A>>) -> anyhow::Result<bool> {
        let _token = self.append_token.acquire().await;

        let (_, prev_index) = entry.prev_clock;
        if let Some(prev_clock) = self.storage.get_entry(prev_index).await?.map(|x| x.this_clock) {
            if prev_clock != entry.prev_clock {
                return Ok(false);
            }
        } else {
            // if the entry is snapshot then we should insert this entry without consistency checks.
            // old entries before the new snapshot will be garbage collected.
            let command = entry.command.clone();
            if std::matches!(command.into(), Command::Snapshot { .. }) {
                let (_, snapshot_index) = entry.this_clock;
                log::warn!(
                    "log is too old. replicated a snapshot (idx={}) from leader",
                    snapshot_index
                );

                if sender_id != core.id && snapshot_index > 1 {
                    let res = core.fetch_snapshot(snapshot_index, sender_id.clone()).await;
                    if res.is_err() {
                        log::error!("could not fetch app snapshot (idx={}) from sender {}", snapshot_index, sender_id);
                        return Ok(false);
                    }
                }

                self.insert_snapshot(entry).await?;
                self.commit_index.store(snapshot_index - 1, Ordering::SeqCst);
                self.last_applied.store(snapshot_index - 1, Ordering::SeqCst);

                return Ok(true);
            } else {
                return Ok(false);
            }
        }

        let (_, new_index) = entry.this_clock;

        if let Some(old_clock) = self.storage.get_entry(new_index).await?.map(|e| e.this_clock) {
            if old_clock == entry.this_clock {
                // if there is a entry with the same term and index
                // then the entry should be the same so skip insertion.
            } else {
                log::warn!("log conflicted at idx: {}", new_index);

                let old_last_log_index = self.storage.get_last_index().await?;
                for idx in new_index..old_last_log_index {
                    self.ack_chans.write().await.remove(&idx);
                }

                self.insert_entry(entry).await?;
            }
        } else {
            self.insert_entry(entry).await?;
        }

        Ok(true)
    }
    async fn insert_entry(&self, e: Entry) -> anyhow::Result<()> {
        let (_, idx) = e.this_clock;
        self.storage.insert_entry(idx, e).await?;
        Ok(())
    }
    async fn insert_snapshot(&self, e: Entry) -> anyhow::Result<()> {
        let (_, idx) = e.this_clock;
        self.storage.insert_snapshot(idx, e).await?;
        Ok(())
    }
    async fn advance_commit_index<A: RaftApp>(&self, new_agreement: Index, core: Arc<RaftCore<A>>) -> anyhow::Result<()> {
        let _token = self.commit_token.acquire().await;

        let old_agreement = self.commit_index.load(Ordering::SeqCst);
        if !(new_agreement > old_agreement) {
            return Ok(());
        }

        for i in old_agreement + 1..=new_agreement {
            let e = self.storage.get_entry(i).await?.unwrap();
            let term = e.this_clock.0;
            match CommandB::deserialize(&e.command) {
                CommandB::ClusterConfiguration { membership } => {
                    let remove_this_node = !membership.contains(&core.id);
                    let is_leader = std::matches!(*core.election_state.read().await, ElectionState::Leader);
                    if remove_this_node && is_leader {
                        *core.election_state.write().await = ElectionState::Follower;
            
                        // if leader node steps down choose one of the follower node to
                        // become candidate immediately so the downtime becomes shorter.
                        core.transfer_leadership().await;
                    } 
                },
                CommandB::Noop => {
                    core.commit_safe_term(term);
                },
                _ => {},
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
        self.commit_notification.lock().await.publish();
        Ok(())
    }
    async fn advance_last_applied<A: RaftApp>(&self, raft_core: Arc<RaftCore<A>>) -> anyhow::Result<()> {
        let (apply_index, apply_entry, command) = {
            let apply_index = self.last_applied.load(Ordering::SeqCst) + 1;
            let mut e = self.storage.get_entry(apply_index).await?.unwrap();
            let command = std::mem::take(&mut e.command);
            (apply_index, e, command)
        };
        let ok = match CommandB::deserialize(&command) {
            CommandB::Snapshot { membership } => {
                let tag = self.storage.get_tag(apply_index).await?;
                log::info!("install app snapshot");
                let res = raft_core.app.install_snapshot(tag.as_ref(), apply_index).await;
                log::info!("install app snapshot (complete)");
                let success = res.is_ok();
                if success {
                    *self.applied_membership.lock().await = membership;
                    true
                } else {
                    false
                }
            }
            CommandB::Req { core, ref message } => {
                let res = if core {
                    let res = raft_core.process_message(message).await;
                    res.map(|x| (x, None))
                } else {
                    raft_core.app.apply_message(message, apply_index).await
                };
                match res {
                    Ok((msg, new_tag)) => {
                        let mut ack_chans = self.ack_chans.write().await;
                        if ack_chans.contains_key(&apply_index) {
                            let ack = ack_chans.get(&apply_index).unwrap();
                            if std::matches!(ack, Ack::OnApply(_)) {
                                if let Ack::OnApply(tx) = ack_chans.remove(&apply_index).unwrap() {
                                    let _ = tx.send(ack::ApplyOk(msg));
                                }
                            }
                        }

                        if let Some(new_tag) = new_tag {
                            // allows orphan snapshot resource to exist.
                            self.storage.put_tag(apply_index, new_tag).await?;

                            let snapshot_entry = Entry {
                                command: Command::Snapshot {
                                    membership: self.applied_membership.lock().await.clone(),
                                }.into(),
                                .. apply_entry
                            };
                            let delay_sec = raft_core.tunable.read().await.compaction_delay_sec;
                            let delay = Duration::from_secs(delay_sec);
                            log::info!("copy snapshot is made and will be inserted in {}s", delay_sec);

                            // do not allow corresponding snapshot tag doesn't exist for existing snapshot entry.
                            self.snapshot_queue.insert(snapshot::InsertSnapshot {
                                e: snapshot_entry,
                            }, delay).await;
                        }
                        true
                    }
                    Err(e) => {
                        log::error!("log apply error: {} (core={})", e, core);
                        false
                    }
                }
            },
            CommandB::ClusterConfiguration { membership } => {
                *self.applied_membership.lock().await = membership;
                true
            },
            _ => true,
        };
        if ok {
            log::debug!("last_applied -> {}", apply_index);
            self.last_applied.store(apply_index, Ordering::SeqCst);
            self.apply_notification.lock().await.publish();
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
        if let Command::Snapshot {
            membership,
        } = cur_snapshot_entry.command.into()
        {
            let mut base_snapshot_index = cur_snapshot_index; 
            let mut new_membership = membership;
            let mut commands = HashMap::new();
            for i in cur_snapshot_index + 1..=new_snapshot_index {
                let command = self.storage.get_entry(i).await?.unwrap().command;
                commands.insert(i, command);
            }
            let mut app_messages = vec![];
            for (i, command) in &commands {
                match CommandB::deserialize(&command) {
                    CommandB::ClusterConfiguration { membership } => {
                        new_membership = membership;
                    }
                    CommandB::Req {
                        core: false,
                        message,
                    } => {
                        app_messages.push(message);
                    }
                    CommandB::Snapshot {
                        membership,
                    } => {
                        base_snapshot_index = *i;
                        new_membership = membership;
                        app_messages = vec![];
                    }
                    _ => {}
                }
            }
            let base_tag = self.storage.get_tag(base_snapshot_index).await?;
            let new_tag = core.app.fold_snapshot(base_tag.as_ref(), app_messages).await?;
            self.storage.put_tag(new_snapshot_index, new_tag).await?;
            let new_snapshot = {
                let mut e = self.storage.get_entry(new_snapshot_index).await?.unwrap();
                e.command = Command::Snapshot {
                    membership: new_membership,
                }.into();
                e
            };
            let delay = Duration::from_secs(core.tunable.read().await.compaction_delay_sec);
            self.snapshot_queue.insert(snapshot::InsertSnapshot { e: new_snapshot }, delay).await;
            Ok(())
        } else {
            unreachable!()
        }
    }
    async fn run_gc<A: RaftApp>(&self, core: Arc<RaftCore<A>>) -> anyhow::Result<()> {
        let r = self.storage.get_snapshot_index().await?;
        log::debug!("gc .. {}", r);

        // delete old snapshots
        let ls: Vec<Index> = self.storage.list_tags().await?.range(..r).map(|x| *x).collect();
        for i in ls {
            if let Some(tag) = self.storage.get_tag(i).await?.clone() {
                core.app.delete_resource(&tag).await?;
                self.storage.delete_tag(i).await?;
            }
        }
        // remove entries
        self.storage.delete_before(r).await?;
        // remove acks
        let ls: Vec<u64> = self.ack_chans.read().await.range(..r).map(|x| *x.0).collect();
        for i in ls {
            self.ack_chans.write().await.remove(&i);
        }
        Ok(())
    }
}
pub async fn start_server<A: RaftApp>(
    core: Arc<RaftCore<A>>,
) -> Result<(), tonic::transport::Error> {
    tokio::spawn(thread::heartbeat::run(Arc::clone(&core)));
    tokio::spawn(thread::commit::run(Arc::clone(&core)));
    tokio::spawn(thread::compaction::run(Arc::clone(&core)));
    tokio::spawn(thread::election::run(Arc::clone(&core)));
    tokio::spawn(thread::execution::run(Arc::clone(&core)));
    tokio::spawn(thread::query_executor::run(Arc::clone(&core)));
    tokio::spawn(thread::gc::run(Arc::clone(&core)));
    tokio::spawn(thread::snapshot_installer::run(Arc::clone(&core)));
    thread::server::run(Arc::clone(&core)).await
}