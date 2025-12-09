use super::*;

pub mod effect;

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
    membership: spin::RwLock<HashSet<NodeAddress>>,
    peer_contexts: spin::RwLock<HashMap<NodeAddress, PeerContexts>>,
    peer_threads: spin::Mutex<HashMap<NodeAddress, ThreadHandles>>,

    state_machine: Read<StateMachine>,
    driver: RaftHandle,

    queue_rx: thread::EventConsumer<thread::QueueEvent>,
    replication_tx: thread::EventProducer<thread::ReplicationEvent>,
}

#[derive(Deref, Clone)]
pub struct Peers(pub Arc<Inner>);
impl Peers {
    pub fn new(
        state_machine: Read<StateMachine>,
        queue_rx: thread::EventConsumer<thread::QueueEvent>,
        replication_tx: thread::EventProducer<thread::ReplicationEvent>,
        driver: RaftHandle,
    ) -> Self {
        let inner = Inner {
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
