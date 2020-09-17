use anyhow::anyhow;
use async_trait::async_trait;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{Mutex, RwLock, Semaphore};

mod ack;
pub mod connection;
pub mod core_message;
mod membership;
mod query_queue;
mod quorum_join;
mod table;
mod thread;
mod thread_drop;

use ack::Ack;
use connection::EndpointConfig;
use thread::news;

pub mod protoimpl {
    tonic::include_proto!("lol_core");
}

#[async_trait]
pub trait RaftApp: Sync + Send + 'static {
    async fn apply_message(&self, request: Message) -> anyhow::Result<Message>;
    async fn install_snapshot(&self, snapshot: Snapshot) -> anyhow::Result<()>;
    async fn fold_snapshot(
        &self,
        old_snapshot: Snapshot,
        requests: Vec<Message>,
    ) -> anyhow::Result<Snapshot>;
}

pub type Message = Vec<u8>;
pub type Snapshot = Option<Vec<u8>>;
type Term = u64;
type Index = u64;
type Clock = (Term, Index);
/// id = ip:port
pub type Id = String;

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
enum CommandProtocol {
    Noop,
    Snapshot {
        app_snapshot: Snapshot,
        core_snapshot: HashSet<String>,
    },
    AddServer {
        id: Id,
    },
    RemoveServer {
        id: Id,
    },
    Req {
        core: bool,
        message: Message,
    },
}
#[derive(Clone, Debug)]
enum Command {
    Noop,
    Snapshot {
        app_snapshot: Option<Vec<u8>>,
        core_snapshot: HashSet<Id>,
    },
    AddServer {
        id: Id,
    },
    RemoveServer {
        id: Id,
    },
    Req {
        core: bool,
        message: Vec<u8>
    }
}
impl From<Vec<u8>> for Command {
    fn from(x: Vec<u8>) -> Self {
        let protocol: CommandProtocol = rmp_serde::from_slice(&x).unwrap();
        match protocol {
            CommandProtocol::Noop => Command::Noop,
            CommandProtocol::Snapshot { app_snapshot, core_snapshot } => Command::Snapshot { app_snapshot, core_snapshot },
            CommandProtocol::AddServer { id } => Command::AddServer { id },
            CommandProtocol::RemoveServer { id } => Command::RemoveServer { id },
            CommandProtocol::Req { core, message } => Command::Req { core, message }
        }
    }
}
impl Into<Vec<u8>> for Command {
    fn into(self) -> Vec<u8> {
        let protocol: CommandProtocol = match self {
            Command::Noop => CommandProtocol::Noop,
            Command::Snapshot { app_snapshot, core_snapshot } => CommandProtocol::Snapshot { app_snapshot, core_snapshot },
            Command::AddServer { id } => CommandProtocol::AddServer { id },
            Command::RemoveServer { id } => CommandProtocol::RemoveServer { id },
            Command::Req { core, message } => CommandProtocol::Req { core, message }
        };
        rmp_serde::to_vec(&protocol).unwrap()
    }
}
#[derive(Clone, Debug)]
struct Entry {
    /// when this entry was inserted in this node
    append_time: Instant,
    prev_clock: Clock,
    this_clock: Clock,
    command: Command,
}
#[derive(Clone, Copy)]
enum ElectionState {
    Leader,
    Candidate,
    Follower,
}
#[derive(Clone)]
struct Vote {
    cur_term: Term,
    voted_for: Option<Id>,
    election_state: ElectionState,
}
// static config
pub struct Config {
    pub id: Id,
}
pub struct TunableConfig {
    /// the interval that compaction runs
    pub compaction_interval_sec: u64,
    /// the system memory threshold threshold that compaction runs
    pub compaction_memory_limit: f64,
}
impl TunableConfig {
    pub fn new() -> Self {
        Self {
            compaction_interval_sec: 300,
            compaction_memory_limit: 0.9,
        }
    }
}
pub struct RaftCore<A> {
    id: Id,
    app: A,
    query_queue: Mutex<query_queue::QueryQueue>,
    last_heartbeat_received: Mutex<Instant>,
    log: Log,
    vote: RwLock<Vote>,
    cluster: RwLock<membership::Cluster>,
    tunable: RwLock<TunableConfig>,
    election_token: Semaphore,
}
impl<A: RaftApp> RaftCore<A> {
    pub async fn new(app: A, config: Config, tunable: TunableConfig) -> Self {
        let id = config.id;
        let init_cluster = membership::Cluster::empty(id.clone()).await;
        let init_log = Log::new().await;
        Self {
            app,
            query_queue: Mutex::new(query_queue::QueryQueue::new()),
            id,
            last_heartbeat_received: Mutex::new(Instant::now()),
            log: init_log,
            vote: RwLock::new(Vote {
                cur_term: 0,
                voted_for: None,
                election_state: ElectionState::Follower,
            }),
            cluster: RwLock::new(init_cluster),
            tunable: RwLock::new(tunable),
            election_token: Semaphore::new(1),
        }
    }
    async fn apply_message(self: &Arc<Self>, msg: Message) -> anyhow::Result<Message> {
        let req = core_message::Req::deserialize(&msg).unwrap();
        match req {
            core_message::Req::InitCluster => {
                let res = if self.cluster.read().await.internal.len() == 0 {
                    log::info!("init cluster");
                    self.init_cluster(self.id.clone()).await;
                    core_message::Rep::InitCluster { ok: true }
                } else {
                    core_message::Rep::InitCluster { ok: false }
                };
                Ok(core_message::Rep::serialize(&res))
            }
            core_message::Req::ClusterInfo => {
                let res = core_message::Rep::ClusterInfo {
                    leader_id: self.vote.read().await.voted_for.clone(),
                    membership: {
                        let mut xs: Vec<_> =
                            self.cluster.read().await.id_list().into_iter().collect();
                        xs.sort();
                        xs
                    },
                };
                Ok(core_message::Rep::serialize(&res))
            }
            core_message::Req::LogInfo => {
                let res = core_message::Rep::LogInfo {
                    head_log_index: self.log.head_log_index.load(Ordering::SeqCst),
                    last_applied: self.log.last_applied.load(Ordering::SeqCst),
                    commit_index: self.log.commit_index.load(Ordering::SeqCst),
                    last_log_index: self.log.last_log_index.load(Ordering::SeqCst),
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
    async fn init_cluster(self: &Arc<Self>, id: Id) {
        let snapshot = Entry {
            append_time: Instant::now(),
            prev_clock: (0, 0),
            this_clock: (0, 1),
            command: Command::Snapshot {
                app_snapshot: None,
                core_snapshot: HashSet::new(),
            },
        };
        self.log.try_insert_entry(snapshot, Arc::clone(&self)).await;
        let add_server = Command::AddServer { id };
        self.log.append_new_entry(add_server, None, 0).await;

        self.log.advance_commit_index(2, Arc::clone(&self)).await;
    }
    async fn queue_received_entry(self: &Arc<Self>, req: protoimpl::AppendEntryReq) -> bool {
        let mut prev_clock = (req.prev_log_term, req.prev_log_index);
        for e in &req.entries {
            let command: Command = e.command.clone().into();
            let entry = Entry {
                append_time: Instant::now(),
                prev_clock,
                this_clock: (e.term, e.index),
                command,
            };
            if !self.log.try_insert_entry(entry, Arc::clone(&self)).await {
                log::warn!("rejected append entry (clock={:?})", (e.term, e.index));
                return false;
            }
            prev_clock = (e.term, e.index);
        }
        true
    }
    async fn receive_heartbeat(
        self: &Arc<Self>,
        leader_id: Id,
        leader_term: Term,
        leader_commit: Index,
    ) {
        let mut vote = self.vote.write().await;
        if leader_term < vote.cur_term {
            log::warn!("heartbeat is stale. rejected");
            return;
        }

        if leader_term > vote.cur_term {
            log::warn!("received heartbeat with newer term. reset vote");
            vote.cur_term = leader_term;
            vote.voted_for = None;
            vote.election_state = ElectionState::Follower;
        }

        if vote.voted_for != Some(leader_id.clone()) {
            log::info!("learn the current leader ({})", leader_id);
            vote.voted_for = Some(leader_id);
        }

        *self.last_heartbeat_received.lock().await = Instant::now();

        let new_commit_index = std::cmp::min(
            leader_commit,
            self.log.last_log_index.load(Ordering::SeqCst),
        );
        self.log
            .advance_commit_index(new_commit_index, Arc::clone(&self))
            .await;
    }
    async fn register_query(self: &Arc<Self>, core: bool, message: Message, ack: Ack) {
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
fn into_stream(
    req: crate::protoimpl::AppendEntryReq,
) -> impl futures::stream::Stream<Item = crate::protoimpl::AppendEntryReqS> {
    use crate::protoimpl::{append_entry_req_s::Elem, EntryS, FrameS, HeaderS};
    let mut elems = vec![];

    elems.push(Elem::Header(HeaderS {
        prev_log_index: req.prev_log_index,
        prev_log_term: req.prev_log_term,
    }));

    for e in &req.entries {
        elems.push(Elem::Entry(EntryS {
            term: e.term,
            index: e.index,
        }));
        let mut frames = vec![];
        let command = e.command.clone();
        // the chunk size of 16KB-64KB is known as best in gRPC streaming.
        for chunk in command.chunks(32_000) {
            frames.push(Elem::Frame(FrameS {
                frame: chunk.to_owned(),
            }));
        }
        elems.append(&mut frames);
    }

    futures::stream::iter(
        elems
            .into_iter()
            .map(|x| crate::protoimpl::AppendEntryReqS { elem: Some(x) }),
    )
}
impl<A> RaftCore<A> {
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
                    let req = protoimpl::TimeoutNowReq {};
                    conn.timeout_now(req).await;
                }
            });
        }
    }
    async fn try_promote(&self) {
        self.election_token.acquire().await;

        // vote to self
        let aim_term = {
            let mut writer = self.vote.write().await;
            let cur_term = writer.cur_term;
            let aim_term = cur_term + 1;
            writer.cur_term = aim_term;
            writer.voted_for = Some(self.id.clone());
            writer.election_state = ElectionState::Candidate;
            aim_term
        };

        log::info!("start election. try promote at term {}", aim_term);

        // try to promote at the term.
        self.try_promote_at(aim_term).await;
    }
    async fn prepare_replication_request(
        &self,
        l: Index,
        r: Index,
    ) -> crate::protoimpl::AppendEntryReq {
        let head = self.log.x.get(l, |e| e.clone()).await.unwrap();
        let (prev_log_term, prev_log_index) = head.prev_clock;
        let (term, index) = head.this_clock;
        let command = head.command;
        let e = crate::protoimpl::Entry {
            term,
            index,
            command: command.into(),
        };
        let mut req = crate::protoimpl::AppendEntryReq {
            prev_log_term,
            prev_log_index,
            entries: vec![e],
        };

        for idx in l + 1..r {
            let e = {
                let x = self.log.x.get(idx, |e| e.clone()).await.unwrap();
                let (term, index) = x.this_clock;
                let command = x.command;
                crate::protoimpl::Entry {
                    term,
                    index,
                    command: command.into(),
                }
            };
            req.entries.push(e);
        }

        req
    }
    async fn advance_replication(&self, follower_id: Id) -> bool {
        let member = self
            .cluster
            .read()
            .await
            .internal
            .get(&follower_id)
            .unwrap()
            .clone();

        let old_progress = member.progress.unwrap();
        let cur_last_log_index = self.log.last_log_index.load(Ordering::SeqCst);

        // more entries to send?
        let should_send = cur_last_log_index >= old_progress.next_index;
        if !should_send {
            return false;
        }

        // the entries to send could be deleted due to previous compactions.
        // in this case, replication will reset from the current head index.
        let cur_head_log_index = self.log.head_log_index.load(Ordering::SeqCst);
        if old_progress.next_index < cur_head_log_index {
            log::warn!(
                "entry not found at next_index (idx={}) for {}",
                old_progress.next_index,
                follower_id
            );
            let mut cluster = self.cluster.write().await;
            let new_progress = membership::ReplicationProgress::new(cur_head_log_index);
            cluster.internal.get_mut(&follower_id).unwrap().progress = Some(new_progress);
            return true;
        }

        let n_max_possible = cur_last_log_index - old_progress.next_index + 1;
        let n = std::cmp::min(old_progress.next_max_cnt, n_max_possible);
        assert!(n >= 1);

        let req = self
            .prepare_replication_request(old_progress.next_index, old_progress.next_index + n)
            .await;

        let res = async {
            let endpoint = member.endpoint;
            let mut conn = endpoint.connect().await?;
            conn.send_append_entry_s(into_stream(req)).await
        }
        .await;

        let mut incremented = false;
        let new_progress = if res.is_ok() {
            let res = res.unwrap();
            match res.into_inner() {
                crate::protoimpl::AppendEntryRep { success: true, .. } => {
                    incremented = true;
                    membership::ReplicationProgress {
                        match_index: old_progress.next_index + n - 1,
                        next_index: old_progress.next_index + n,
                        next_max_cnt: n * 2,
                    }
                }
                crate::protoimpl::AppendEntryRep {
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
            self.log.replication_news.lock().await.publish();
        }

        true
    }
    async fn broadcast_heartbeat(&self) {
        let cluster = self.cluster.read().await.internal.clone();
        let mut futs = vec![];
        for (id, member) in cluster {
            if id == self.id {
                continue;
            }
            let endpoint = member.endpoint;
            let config = EndpointConfig::default().timeout(Duration::from_millis(100));
            let req = {
                let term = self.vote.read().await.cur_term;
                protoimpl::HeartbeatReq {
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
    }
    async fn receive_vote(
        &self,
        candidate_term: Term,
        candidate_id: Id,
        candidate_last_log_clock: Clock,
    ) -> bool {
        let mut vote = self.vote.write().await;
        if candidate_term < vote.cur_term {
            log::warn!("candidate term is older. reject vote");
            return false;
        }

        if candidate_term > vote.cur_term {
            log::warn!("received newer term. reset vote");
            vote.cur_term = candidate_term;
            vote.voted_for = None;
            vote.election_state = ElectionState::Follower;
        }

        let this_last_log_clock = self
            .log
            .x
            .get(self.log.last_log_index.load(Ordering::SeqCst), |e| {
                e.this_clock
            })
            .await
            .unwrap();
        if candidate_last_log_clock < this_last_log_clock {
            log::warn!("candidate clock is older. reject vote");
            return false;
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

        log::info!("voted response to {} = grant: {}", candidate_id, grant);
        grant
    }
    async fn find_new_agreement(&self) -> Index {
        let cluster = self.cluster.read().await.internal.clone();
        let new_agreement = {
            let n = cluster.len();
            let mid = n / 2;
            let mut match_indices = vec![];
            for (id, member) in cluster {
                if id == self.id {
                    let last_log_index = self.log.last_log_index.load(Ordering::SeqCst);
                    match_indices.push(last_log_index);
                } else {
                    match_indices.push(member.progress.unwrap().match_index);
                }
            }
            match_indices.sort();
            match_indices.reverse();
            match_indices[mid]
        };
        new_agreement
    }
    async fn try_promote_at(&self, aim_term: Term) {
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

        let last_log_index = self.log.last_log_index.load(Ordering::SeqCst);
        let last_log_clock = self
            .log
            .x
            .get(last_log_index, |e| e.this_clock)
            .await
            .unwrap();

        let timeout = Duration::from_secs(5);
        let mut vote_requests = vec![];
        for endpoint in others {
            let myid = self.id.clone();
            vote_requests.push(async move {
                let (last_log_term, last_log_index) = last_log_clock;
                let req = crate::protoimpl::RequestVoteReq {
                    term: aim_term,
                    candidate_id: myid,
                    last_log_term,
                    last_log_index,
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
        if ok {
            log::info!("got enough votes from the cluster. promoted to leader");

            // initialize replication progress
            {
                let initial_progress = membership::ReplicationProgress::new(
                    self.log.last_log_index.load(Ordering::SeqCst),
                );
                let mut cluster = self.cluster.write().await;
                for (id, member) in &mut cluster.internal {
                    if id != &self.id {
                        member.progress = Some(initial_progress.clone());
                    }
                }
            }

            // become the leader of the aim term because the vote was done for the term.
            {
                let mut vote = self.vote.write().await;
                vote.cur_term = aim_term;
                vote.voted_for = Some(self.id.clone());
                vote.election_state = ElectionState::Leader;
            }

            // as soon as the node becomes the leader, replicate noop entries with term
            self.queue_entry(Command::Noop, None).await;

            self.broadcast_heartbeat().await;
        } else {
            log::info!("failed to become leader. now back to follower");
            let mut vote = self.vote.write().await;
            vote.election_state = ElectionState::Follower;
        }
    }
    async fn queue_entry(&self, command: Command, ack: Option<Ack>) {
        let term = self.vote.read().await.cur_term;
        self.log.append_new_entry(command, ack, term).await;
    }
}
struct Log {
    x: table::Table<Entry>,
    ack_chans: RwLock<HashMap<Index, Ack>>,

    head_log_index: AtomicU64, // monotonic
    last_applied: AtomicU64,   // monotonic
    commit_index: AtomicU64,   // monotonic
    last_log_index: AtomicU64,

    append_token: Semaphore,
    commit_token: Semaphore,
    compaction_token: Semaphore,

    append_news: Mutex<news::News>,
    replication_news: Mutex<news::News>,
    commit_news: Mutex<news::News>,
    apply_news: Mutex<news::News>,
}
impl Log {
    async fn new() -> Self {
        let h = table::Table::new();
        Self {
            x: h,
            ack_chans: RwLock::new(HashMap::new()),

            head_log_index: 0.into(),
            last_applied: 0.into(),
            commit_index: 0.into(),
            last_log_index: 0.into(),

            append_token: Semaphore::new(1),
            commit_token: Semaphore::new(1),
            compaction_token: Semaphore::new(1),

            append_news: Mutex::new(news::News::new()),
            replication_news: Mutex::new(news::News::new()),
            commit_news: Mutex::new(news::News::new()),
            apply_news: Mutex::new(news::News::new()),
        }
    }
    async fn append_new_entry(&self, command: Command, ack: Option<Ack>, term: Term) {
        let _token = self.append_token.acquire().await;

        let cur_last_log_index = self.last_log_index.load(Ordering::SeqCst);
        let prev_clock = self
            .x
            .get(cur_last_log_index, |e| e.this_clock)
            .await
            .unwrap();
        let new_index = cur_last_log_index + 1;
        let this_clock = (term, new_index);
        let e = Entry {
            append_time: Instant::now(),
            prev_clock,
            this_clock,
            command,
        };
        self.x.insert(new_index, e).await;
        if let Some(x) = ack {
            self.ack_chans.write().await.insert(new_index, x);
        }
        self.last_log_index.store(new_index, Ordering::SeqCst);
        self.append_news.lock().await.publish();
    }
    async fn try_insert_entry<A: RaftApp>(&self, mut entry: Entry, core: Arc<RaftCore<A>>) -> bool {
        let _token = self.append_token.acquire().await;

        let (_, prev_index) = entry.prev_clock;
        if let Some(prev_clock) = self.x.get(prev_index, |e| e.this_clock).await {
            if prev_clock != entry.prev_clock {
                return false;
            }
        } else {
            // if the entry is snapshot then we should insert this entry without consistency checks.
            // old entries before the new snapshot will be garbage collected.
            if std::matches!(entry.command, Command::Snapshot { .. }) {
                let (_, new_index) = entry.this_clock;
                log::warn!(
                    "log is too old. replicated a snapshot (idx={}) from leader",
                    new_index
                );

                entry.append_time = Instant::now();
                self.x.insert(new_index, entry).await;
                self.last_log_index.store(new_index, Ordering::SeqCst);
                self.commit_index.store(new_index - 1, Ordering::SeqCst);
                self.last_applied.store(new_index - 1, Ordering::SeqCst);
                let old_head_index = self.head_log_index.fetch_max(new_index, Ordering::SeqCst);

                tokio::spawn({
                    let core = Arc::clone(&core);
                    async move {
                        for idx in old_head_index..new_index {
                            core.log.x.remove(idx).await;
                            core.log.ack_chans.write().await.remove(&idx);
                        }
                    }
                });

                return true;
            } else {
                return false;
            }
        }

        let (_, new_index) = entry.this_clock;

        if let Some(old_clock) = self.x.get(new_index, |e| e.this_clock).await {
            if old_clock == entry.this_clock {
                // if there is a entry with the same term and index
                // then the entry should be the same so skip insertion.
            } else {
                log::warn!("log conflicted at idx: {}", new_index);

                let old_last_log_index = self.last_log_index.load(Ordering::SeqCst);
                for idx in new_index..old_last_log_index {
                    self.x.remove(idx).await;
                    self.ack_chans.write().await.remove(&idx);
                }

                entry.append_time = Instant::now();
                self.x.insert(new_index, entry).await;
                self.last_log_index.store(new_index, Ordering::SeqCst);
            }
        } else {
            entry.append_time = Instant::now();
            self.x.insert(new_index, entry).await;
            self.last_log_index.store(new_index, Ordering::SeqCst);
        }

        true
    }
    async fn advance_commit_index<A: RaftApp>(&self, new_agreement: Index, core: Arc<RaftCore<A>>) {
        let _token = self.commit_token.acquire().await;

        let old_agreement = self.commit_index.load(Ordering::SeqCst);
        if !(new_agreement > old_agreement) {
            return;
        }

        for i in old_agreement + 1..=new_agreement {
            let command = self.x.get(i, |e| e.command.clone()).await.unwrap();
            match command {
                Command::AddServer { id } => {
                    log::info!("add-server: {}", id);
                    core.cluster
                        .write()
                        .await
                        .add_server(id.clone(), Arc::clone(&core))
                        .await;
                }
                Command::RemoveServer { id } => {
                    log::info!("remove-server: {}", id);
                    core.cluster.write().await.remove_server(id.clone());
                    let remove_leader = id == core.id
                        && std::matches!(
                            core.vote.read().await.election_state,
                            ElectionState::Leader
                        );
                    if remove_leader {
                        core.vote.write().await.election_state = ElectionState::Follower;

                        // if leader node steps down choose one of the follower node to
                        // become candidate immediately so the downtime becomes shorter.
                        core.transfer_leadership().await;
                    }
                }
                Command::Snapshot { core_snapshot, .. } => {
                    log::info!("install core snapshot: {:?}", core_snapshot);
                    core.cluster
                        .write()
                        .await
                        .set_membership(&core_snapshot, Arc::clone(&core))
                        .await;
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

        self.commit_index.store(new_agreement, Ordering::SeqCst);
        self.commit_news.lock().await.publish();
    }
    async fn advance_last_applied<A: RaftApp>(&self, raft_core: Arc<RaftCore<A>>) {
        let (command, apply_idx) = {
            let apply_idx = self.last_applied.load(Ordering::SeqCst) + 1;
            let command = self.x.get(apply_idx, |e| e.command.clone()).await.unwrap();
            (command, apply_idx)
        };
        let ok = match command {
            Command::Snapshot { app_snapshot, .. } => {
                log::info!("install app snapshot");
                let res = raft_core.app.install_snapshot(app_snapshot).await;
                log::info!("install app snapshot (complete)");
                res.is_ok()
            }
            Command::Req { message, core } => {
                let res = if core {
                    raft_core.apply_message(message).await
                } else {
                    raft_core.app.apply_message(message).await
                };
                match res {
                    Ok(msg) => {
                        let mut ack_chans = self.ack_chans.write().await;
                        if ack_chans.contains_key(&apply_idx) {
                            let ack = ack_chans.get(&apply_idx).unwrap();
                            if std::matches!(ack, Ack::OnApply(_)) {
                                if let Ack::OnApply(tx) = ack_chans.remove(&apply_idx).unwrap() {
                                    let _ = tx.send(ack::ApplyOk(msg));
                                }
                            }
                        }
                        true
                    }
                    Err(e) => {
                        log::error!("log apply error: {} (core={})", e, core);
                        false
                    }
                }
            }
            _ => true,
        };
        if ok {
            self.last_applied.store(apply_idx, Ordering::SeqCst);
            self.apply_news.lock().await.publish();
        }
    }
    async fn find_compaction_point(&self, guard_period: Duration) -> Option<Index> {
        let last_applied = self.last_applied.load(Ordering::SeqCst);
        let now = Instant::now();
        let new_head_index = {
            let mut res = None;
            // find a compaction point before last_applied but old enough so fresh entries
            // will be replicated to slower nodes.
            for i in (self.head_log_index.load(Ordering::SeqCst) + 1..=last_applied).rev() {
                let append_time = self.x.get(i, |e| e.append_time).await.unwrap();
                if now - append_time < guard_period {
                    // fresh entries will not be a target of compaction.
                } else {
                    res = Some(i);
                    break;
                }
            }
            res
        };
        new_head_index
    }
    async fn advance_head_log_index<A: RaftApp>(
        &self,
        new_head_index: Index,
        core: Arc<RaftCore<A>>,
    ) {
        assert!(new_head_index <= self.last_applied.load(Ordering::SeqCst));

        let _token = self.compaction_token.acquire().await;

        let cur_head_log_index = self.head_log_index.load(Ordering::SeqCst);

        if new_head_index <= cur_head_log_index {
            return;
        }

        log::info!(
            "advance head index {} -> {}",
            cur_head_log_index,
            new_head_index
        );
        let cur_snapshot = self
            .x
            .get(cur_head_log_index, |e| e.command.clone())
            .await
            .unwrap();
        if let Command::Snapshot {
            app_snapshot,
            core_snapshot,
        } = cur_snapshot
        {
            let mut new_app_snapshot = app_snapshot;
            let mut new_core_snapshot = core_snapshot;
            let mut app_messages = vec![];
            for i in cur_head_log_index + 1..=new_head_index {
                match self.x.get(i, |e| e.command.clone()).await.unwrap() {
                    Command::AddServer { id } => {
                        log::info!("snapshot fold: add-server({})", id);
                        new_core_snapshot.insert(id);
                    }
                    Command::RemoveServer { id } => {
                        log::info!("snapshot fold: remove-server({})", id);
                        new_core_snapshot.remove(&id);
                    }
                    Command::Req {
                        message,
                        core: false,
                    } => {
                        app_messages.push(message);
                    }
                    Command::Snapshot {
                        app_snapshot,
                        core_snapshot,
                    } => {
                        new_app_snapshot = app_snapshot;
                        new_core_snapshot = core_snapshot;
                        app_messages = vec![];
                    }
                    _ => {}
                }
            }

            new_app_snapshot = match core.app.fold_snapshot(new_app_snapshot, app_messages).await {
                Ok(x) => x,
                Err(_) => {
                    log::error!("failed to create new snapshot");
                    return;
                }
            };

            let new_head = {
                let mut e = self.x.get(new_head_index, |e| e.clone()).await.unwrap();
                e.command = Command::Snapshot {
                    app_snapshot: new_app_snapshot,
                    core_snapshot: new_core_snapshot,
                };
                e
            };
            self.x.insert(new_head_index, new_head).await;
            let old_head_index = self
                .head_log_index
                .fetch_max(new_head_index, Ordering::SeqCst);

            log::info!(
                "remove old entries in [{}, {})",
                old_head_index,
                new_head_index
            );
            tokio::spawn(async move {
                for idx in old_head_index..new_head_index {
                    core.log.x.remove(idx).await;
                    assert!(!core.log.ack_chans.read().await.contains_key(&idx));
                }
            });
        } else {
            unreachable!()
        }
    }
}
pub async fn start_server<A: RaftApp>(
    core: Arc<RaftCore<A>>,
) -> Result<(), tonic::transport::Error> {
    tokio::spawn(thread::heartbeat::run(Arc::clone(&core)));
    tokio::spawn(thread::commit::run(Arc::clone(&core)));
    tokio::spawn(thread::compaction_l1::run(Arc::clone(&core)));
    tokio::spawn(thread::compaction_l2::run(Arc::clone(&core)));
    tokio::spawn(thread::election::run(Arc::clone(&core)));
    tokio::spawn(thread::execution::run(Arc::clone(&core)));
    tokio::spawn(thread::query_executor::run(Arc::clone(&core)));
    thread::server::run(Arc::clone(&core)).await
}
