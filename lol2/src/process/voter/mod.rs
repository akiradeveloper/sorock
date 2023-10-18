use super::*;

mod election;
mod failure_detector;
mod heartbeat;
mod quorum;
mod stepdown;

#[derive(Clone, Copy, Debug)]
pub enum ElectionState {
    Leader,
    Candidate,
    Follower,
}

pub struct Inner {
    state: std::sync::Mutex<ElectionState>,
    ballot: Box<dyn RaftBallotStore>,

    /// Serializing any events that may change ballot state simplifies the voter's logic.
    vote_lock: tokio::sync::Mutex<()>,

    /// Unless `safe_term >= cur_term`,
    /// any new entries are not allowed to be queued.
    safe_term: AtomicU64,

    leader_failure_detector: failure_detector::FailureDetector,

    command_log: CommandLog,
    peers: PeerSvc,
    driver: RaftDriver,
}

#[derive(shrinkwraprs::Shrinkwrap, Clone)]
pub struct Voter(pub Arc<Inner>);
impl Voter {
    pub fn new(
        ballot_store: impl RaftBallotStore,
        command_log: CommandLog,
        peers: PeerSvc,
        driver: RaftDriver,
    ) -> Self {
        let inner = Inner {
            state: std::sync::Mutex::new(ElectionState::Follower),
            ballot: Box::new(ballot_store),
            vote_lock: tokio::sync::Mutex::new(()),
            safe_term: AtomicU64::new(0),
            leader_failure_detector: failure_detector::FailureDetector::new(),
            command_log,
            peers,
            driver,
        };
        Self(Arc::new(inner))
    }
}

impl Voter {
    pub fn read_election_state(&self) -> ElectionState {
        *self.state.lock().unwrap()
    }
    pub fn write_election_state(&self, e: ElectionState) {
        info!("election state -> {e:?}");
        *self.state.lock().unwrap() = e;
    }

    pub async fn read_ballot(&self) -> Result<Ballot> {
        self.ballot.load_ballot().await
    }

    pub async fn write_ballot(&self, b: Ballot) -> Result<()> {
        self.ballot.save_ballot(b).await
    }

    pub fn commit_safe_term(&self, term: Term) {
        info!("commit safe term={term}");
        self.safe_term.store(term, Ordering::SeqCst);
    }

    pub async fn allow_queue_entry(&self) -> Result<bool> {
        let cur_term = self.ballot.load_ballot().await?.cur_term;
        let cur_safe_term = self.safe_term.load(Ordering::SeqCst);
        Ok(cur_safe_term == cur_term)
    }
}
