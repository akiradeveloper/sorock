use super::*;

pub mod effect;
mod failure_detector;
mod init;
mod quorum;
mod replication;
pub mod thread;
use replication::Replication;

#[derive(Clone, Copy, Debug)]
pub enum ElectionState {
    Leader,
    Candidate,
    Follower,
}

#[allow(dead_code)]
struct ThreadHandles {
    replicator_handle: ThreadHandle,
    heartbeater_handle: ThreadHandle,
}

pub struct Control {
    // voter
    state: ElectionState,
    ballot: storage::BallotStore,
    safe_term: u64,
    leader_failure_detector: failure_detector::FailureDetector,
    pub commit_pointer: u64,

    // peers
    membership: HashMap<ServerAddress, bool>,
    replication_contexts: HashMap<ServerAddress, Actor<Replication>>,
    peer_threads: HashMap<ServerAddress, ThreadHandles>,
    queue_evt_rx: EventWaiter<QueueEvent>,
    replication_evt_tx: EventNotifier<ReplicationEvent>,
    /// The index of the last membership.
    /// Unless `commit_pointer` >= membership_pointer`,
    /// new membership changes are not allowed to be queued.
    membership_pointer: u64,

    command_log_actor: Actor<CommandLog>,

    io: RaftIO,
}

impl Control {
    pub fn new(
        ballot_store: storage::BallotStore,
        command_log: Actor<CommandLog>,
        queue_evt_rx: EventWaiter<QueueEvent>,
        replication_evt_tx: EventNotifier<ReplicationEvent>,
        io: RaftIO,
    ) -> Self {
        Self {
            state: ElectionState::Follower,
            ballot: ballot_store,
            safe_term: 0,
            leader_failure_detector: failure_detector::FailureDetector::new(),
            commit_pointer: 0,

            membership_pointer: 0,
            membership: HashMap::new(),
            replication_contexts: HashMap::new(),
            peer_threads: HashMap::new(),
            queue_evt_rx,
            replication_evt_tx,

            command_log_actor: command_log,
            io,
        }
    }

    pub fn is_leader(&self) -> bool {
        matches!(self.state, ElectionState::Leader)
    }

    fn write_election_state(&mut self, e: ElectionState) {
        info!("election state -> {e:?}");
        self.state = e;
    }

    pub async fn read_ballot(&self) -> Result<Ballot> {
        self.ballot.load_ballot().await
    }

    async fn write_ballot(&mut self, b: Ballot) -> Result<()> {
        self.ballot.save_ballot(b).await
    }

    pub fn commit_safe_term(&mut self, term: Term) {
        info!("commit safe term={term}");
        self.safe_term = term;
    }

    /// If `safe_term < cur_term`, any new entries are not allowed to be queued.
    pub async fn allow_queue_new_entry(&self) -> Result<bool> {
        let cur_term = self.ballot.load_ballot().await?.cur_term;
        let cur_safe_term = self.safe_term;
        Ok(cur_safe_term == cur_term)
    }

    pub fn allow_queue_new_membership(&self) -> bool {
        self.commit_pointer >= self.membership_pointer
    }

    pub fn get_election_timeout(&self) -> Option<Duration> {
        if !matches!(self.state, ElectionState::Follower) {
            return None;
        }

        // This is an optimization to avoid unnecessary election.
        // If the node doesn't contain itself in its voting membership, it can't become a new leader anyway.
        if !self.is_voter(&self.io.local_server_id) {
            return None;
        }

        self.leader_failure_detector.get_election_timeout()
    }

    async fn send_heartbeat(&self, follower_id: ServerAddress) -> Result<()> {
        let ballot = self.read_ballot().await?;
        let cur_commit_index = self.commit_pointer;
        let req = request::Heartbeat {
            sender_term: ballot.cur_term,
            sender_commit_index: cur_commit_index,
        };
        let conn = self.io.connect(&follower_id);
        conn.queue_heartbeat(req);
        Ok(())
    }

    pub fn read_membership(&self) -> HashMap<ServerAddress, bool> {
        self.membership.clone()
    }

    pub fn is_voter(&self, server_id: &ServerAddress) -> bool {
        *self.membership.get(server_id).unwrap_or(&false)
    }

    pub async fn find_new_commit_index(&self) -> Result<LogIndex> {
        let mut match_indices = vec![];

        if self.is_voter(&self.io.local_server_id) {
            // Include self match_index
            let last_log_index = self.command_log_actor.read().await.tail_pointer;
            match_indices.push(last_log_index);
        }

        for (id, peer) in &self.replication_contexts {
            if !self.is_voter(&id) {
                continue;
            }

            let peer = peer.clone();
            // We use try_lock here to avoid waiting for slow followers.
            let match_index = peer.try_read().map(|p| p.match_index).unwrap_or(0);
            match_indices.push(match_index);
        }

        match_indices.sort_unstable();
        match_indices.reverse();
        let mid = match_indices.len() / 2;
        let new_commit_index = match_indices[mid];

        Ok(new_commit_index)
    }

    pub fn advance_commit_index(&mut self, new_commit_index: LogIndex) {
        self.commit_pointer = u64::max(self.commit_pointer, new_commit_index);
    }

    /// Choose the most advanced follower and send it TimeoutNow.
    pub async fn transfer_leadership(&self) -> Result<()> {
        let mut xs = {
            let mut out = vec![];
            for (id, ctx) in &self.replication_contexts {
                if !self.is_voter(&id) {
                    continue;
                }

                let progress = ctx.read().await;
                out.push((id.clone(), progress.match_index));
            }
            out
        };
        // highest match_index in the last
        xs.sort_by_key(|x| x.1);

        if let Some(new_leader) = xs.pop() {
            info!("transfer leadership to {}", new_leader.0);
            let conn = self.io.connect(&new_leader.0);
            conn.send_timeout_now().await?;
        }

        Ok(())
    }

    pub async fn find_read_index(&self) -> Result<Option<LogIndex>> {
        // Issuing read-index should be as safe as queuing a new read command.
        if !self.allow_queue_new_entry().await? {
            return Ok(None);
        }

        if !self.is_voter(&self.io.local_server_id) {
            return Ok(None);
        }

        let cur_term = self.read_ballot().await?.cur_term;

        // A commit-index can be selected as a safety point for readers which we call "read-index".
        // Readers can process read requests before this point.
        let saved = self.commit_pointer;

        let peers = self.membership.clone();

        // Need to confirm majority of peers have terms less than or equal to `cur_term`.
        // This ensures that this node has maintained leadership when commit-index was saved.
        let mut futs = vec![];
        for (peer, _) in peers {
            if !self.is_voter(&peer) {
                continue;
            }

            // Ack from local node is implicit.
            if peer == self.io.local_server_id {
                continue;
            }

            let conn = self.io.connect(&peer);
            let fut = async move {
                let resp = conn.compare_term(cur_term).await;
                // Treat errors as NACK.
                resp.unwrap_or(false)
            };
            futs.push(fut);
        }

        let n = futs.len();
        let ok = quorum::join(n / 2, futs).await;
        if ok {
            Ok(Some(saved))
        } else {
            Ok(None)
        }
    }

    pub async fn compare_term(&self, term: Term) -> Result<bool> {
        let ballot = self.read_ballot().await?;
        Ok(term >= ballot.cur_term)
    }
}
