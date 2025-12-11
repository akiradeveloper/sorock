use crossbeam::queue;

use super::*;

pub mod effect;
mod failure_detector;
mod quorum;

#[derive(Clone, Copy, Debug)]
pub enum ElectionState {
    Leader,
    Candidate,
    Follower,
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct ReplicationProgress {
    /// The log entrires `[0, match_index]` are replicated with this node.
    pub match_index: LogIndex,
    /// In the next replication, log entrires `[next_index, next_index + next_max_cnt)` will be sent.
    pub next_index: LogIndex,
    pub next_max_cnt: LogIndex,
}
impl ReplicationProgress {
    pub fn new(init_next_index: LogIndex) -> Self {
        Self {
            match_index: 0,
            next_index: init_next_index,
            next_max_cnt: 1,
        }
    }
}

#[derive(Clone)]
pub struct PeerContexts {
    progress: ReplicationProgress,
}

#[allow(dead_code)]
struct ThreadHandles {
    replicator_handle: thread::ThreadHandle,
    heartbeater_handle: thread::ThreadHandle,
}

pub struct Inner {
    // voter
    vote_sequencer: tokio::sync::Semaphore,
    state: spin::Mutex<ElectionState>,
    ballot: storage::BallotStore,
    safe_term: AtomicU64,
    leader_failure_detector: failure_detector::FailureDetector,

    // peers
    membership: spin::RwLock<HashSet<NodeAddress>>,
    peer_contexts: spin::RwLock<HashMap<NodeAddress, PeerContexts>>,
    peer_threads: spin::Mutex<HashMap<NodeAddress, ThreadHandles>>,
    queue_rx: thread::EventConsumer<thread::QueueEvent>,
    replication_tx: thread::EventProducer<thread::ReplicationEvent>,

    /// The index of the last membership.
    /// Unless `commit_pointer` >= membership_pointer`,
    /// new membership changes are not allowed to be queued.
    pub membership_pointer: AtomicU64,

    state_machine: Read<StateMachine>,

    driver: RaftHandle,
}

#[derive(Deref, Clone)]
pub struct Control(pub Arc<Inner>);
impl Control {
    pub fn new(
        ballot_store: storage::BallotStore,
        state_machine: Read<StateMachine>,
        queue_rx: thread::EventConsumer<thread::QueueEvent>,
        replication_tx: thread::EventProducer<thread::ReplicationEvent>,
        driver: RaftHandle,
    ) -> Self {
        let inner = Inner {
            state: spin::Mutex::new(ElectionState::Follower),
            ballot: ballot_store,
            vote_sequencer: tokio::sync::Semaphore::new(1),
            safe_term: AtomicU64::new(0),
            leader_failure_detector: failure_detector::FailureDetector::new(),

            membership_pointer: AtomicU64::new(0),
            membership: HashSet::new().into(),
            peer_contexts: HashMap::new().into(),
            peer_threads: HashMap::new().into(),
            queue_rx,
            replication_tx,

            state_machine,
            driver,
        };
        Self(Arc::new(inner))
    }

    pub fn read_election_state(&self) -> ElectionState {
        *self.state.lock()
    }

    fn write_election_state(&self, e: ElectionState) {
        info!("election state -> {e:?}");
        *self.state.lock() = e;
    }

    pub async fn read_ballot(&self) -> Result<Ballot> {
        self.ballot.load_ballot().await
    }

    async fn write_ballot(&self, b: Ballot) -> Result<()> {
        self.ballot.save_ballot(b).await
    }

    pub fn commit_safe_term(&self, term: Term) {
        info!("commit safe term={term}");
        self.safe_term.store(term, Ordering::SeqCst);
    }

    /// If `safe_term < cur_term`, any new entries are not allowed to be queued.
    pub async fn allow_queue_new_entry(&self) -> Result<bool> {
        let cur_term = self.ballot.load_ballot().await?.cur_term;
        let cur_safe_term = self.safe_term.load(Ordering::SeqCst);
        Ok(cur_safe_term == cur_term)
    }

    pub fn allow_queue_new_membership(&self) -> bool {
        self.state_machine.commit_pointer.load(Ordering::SeqCst)
            >= self.membership_pointer.load(Ordering::SeqCst)
    }

    pub fn get_election_timeout(&self) -> Option<Duration> {
        // This is an optimization to avoid unnecessary election.
        // If the node doesn't contain itself in its membership,
        // it can't become a new leader anyway.
        if !self.read_membership().contains(&self.driver.self_node_id()) {
            return None;
        }
        self.leader_failure_detector.get_election_timeout()
    }

    pub async fn send_heartbeat(&self, follower_id: NodeAddress) -> Result<()> {
        let ballot = self.read_ballot().await?;
        let leader_commit_index = self.state_machine.commit_pointer.load(Ordering::SeqCst);
        let req = request::Heartbeat {
            leader_term: ballot.cur_term,
            leader_commit_index,
        };
        let conn = self.driver.connect(follower_id);
        conn.queue_heartbeat(req);
        Ok(())
    }

    pub fn read_membership(&self) -> HashSet<NodeAddress> {
        self.membership.read().clone()
    }

    pub async fn find_new_commit_index(&self) -> Result<LogIndex> {
        let mut match_indices = vec![];

        let last_log_index = self.state_machine.get_log_last_index().await?;
        match_indices.push(last_log_index);

        let peer_contexts = self.peer_contexts.read();
        for (_, peer) in peer_contexts.iter() {
            match_indices.push(peer.progress.match_index);
        }

        match_indices.sort();
        match_indices.reverse();
        let mid = match_indices.len() / 2;
        let new_commit_index = match_indices[mid];

        Ok(new_commit_index)
    }

    /// Choose the most advanced follower and send it TimeoutNow.
    pub async fn transfer_leadership(&self) -> Result<()> {
        let mut xs = {
            let peer_contexts = self.peer_contexts.read();
            let mut out = vec![];
            for (id, peer) in peer_contexts.iter() {
                let progress = peer.progress;
                out.push((id.clone(), progress.match_index));
            }
            out
        };
        // highest match_index in the last
        xs.sort_by_key(|x| x.1);

        if let Some(new_leader) = xs.pop() {
            info!("transfer leadership to {}", new_leader.0);
            let conn = self.driver.connect(new_leader.0.clone());
            conn.send_timeout_now().await?;
        }

        Ok(())
    }
}
