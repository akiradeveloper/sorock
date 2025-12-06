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

pub struct Inner {
    state: spin::Mutex<ElectionState>,
    ballot: Box<dyn RaftBallotStore>,

    /// Serializing any events that may change ballot state simplifies the voter's logic.
    vote_lock: tokio::sync::Mutex<()>,

    safe_term: AtomicU64,

    leader_failure_detector: failure_detector::FailureDetector,

    state_mechine: Read<StateMachine>,
    peers: Read<Peers>,
    driver: RaftHandle,
}

#[derive(Deref, Clone)]
pub struct Voter(pub Arc<Inner>);
impl Voter {
    pub fn new(
        ballot_store: impl RaftBallotStore,
        state_mechine: Read<StateMachine>,
        peers: Read<Peers>,
        driver: RaftHandle,
    ) -> Self {
        let inner = Inner {
            state: spin::Mutex::new(ElectionState::Follower),
            ballot: Box::new(ballot_store),
            vote_lock: tokio::sync::Mutex::new(()),
            safe_term: AtomicU64::new(0),
            leader_failure_detector: failure_detector::FailureDetector::new(),
            state_mechine,
            peers,
            driver,
        };
        Self(Arc::new(inner))
    }
}

impl Voter {
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

    pub fn get_election_timeout(&self) -> Option<Duration> {
        // This is an optimization to avoid unnecessary election.
        // If the node doesn't contain itself in its membership,
        // it can't become a new leader anyway.
        if !self
            .peers
            .read_membership()
            .contains(&self.driver.self_node_id())
        {
            return None;
        }
        self.leader_failure_detector.get_election_timeout()
    }

    pub async fn send_heartbeat(&self, follower_id: NodeId) -> Result<()> {
        let ballot = self.read_ballot().await?;
        let leader_commit_index = self.state_mechine.commit_pointer.load(Ordering::SeqCst);
        let req = request::Heartbeat {
            leader_term: ballot.cur_term,
            leader_commit_index,
        };
        let conn = self.driver.connect(follower_id);
        conn.queue_heartbeat(req);
        Ok(())
    }
}
