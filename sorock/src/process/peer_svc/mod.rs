use super::*;

mod replication;

#[derive(Clone, Copy, Debug)]
pub(crate) struct ReplicationProgress {
    /// The log entrires `[0, match_index]` are replicated with this node.
    pub match_index: Index,
    /// In the next replication, log entrires `[next_index, next_index + next_max_cnt)` will be sent.
    pub next_index: Index,
    pub next_max_cnt: Index,
}
impl ReplicationProgress {
    pub fn new(init_next_index: Index) -> Self {
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
    membership: spin::RwLock<HashSet<NodeId>>,
    peer_contexts: spin::RwLock<HashMap<NodeId, PeerContexts>>,
    peer_threads: spin::Mutex<HashMap<NodeId, ThreadHandles>>,

    command_log: Ref<CommandLog>,
    driver: RaftDriver,

    queue_rx: thread::EventConsumer<thread::QueueEvent>,
    replication_tx: thread::EventProducer<thread::ReplicationEvent>,
}

#[derive(shrinkwraprs::Shrinkwrap, Clone)]
pub struct PeerSvc(pub Arc<Inner>);
impl PeerSvc {
    pub fn new(
        command_log: Ref<CommandLog>,
        queue_rx: thread::EventConsumer<thread::QueueEvent>,
        replication_tx: thread::EventProducer<thread::ReplicationEvent>,
        driver: RaftDriver,
    ) -> Self {
        let inner = Inner {
            membership: HashSet::new().into(),
            peer_contexts: HashMap::new().into(),
            peer_threads: HashMap::new().into(),
            queue_rx,
            replication_tx,
            command_log,
            driver,
        };
        Self(Arc::new(inner))
    }

    /// Restore the membership from the state of the log.
    pub async fn restore_state(&self, voter: Ref<Voter>) -> Result<()> {
        let log_last_index = self.command_log.get_log_last_index().await?;
        let last_membership_index = self
            .command_log
            .find_last_membership_index(log_last_index)
            .await?;

        if let Some(last_membership_index) = last_membership_index {
            let last_membership = {
                let entry = self.command_log.get_entry(last_membership_index).await?;
                match Command::deserialize(&entry.command) {
                    Command::Snapshot { membership } => membership,
                    Command::ClusterConfiguration { membership } => membership,
                    _ => unreachable!(),
                }
            };
            self.set_membership(last_membership, last_membership_index, voter)
                .await?;
        };

        Ok(())
    }
}

impl PeerSvc {
    async fn add_peer(&self, id: NodeId, voter: Ref<Voter>) -> Result<()> {
        if id == self.driver.self_node_id() {
            return Ok(());
        }

        if self.peer_contexts.read().contains_key(&id) {
            return Ok(());
        }

        let init_progress = {
            let last_log_index = self.command_log.get_log_last_index().await?;
            ReplicationProgress::new(last_log_index)
        };

        let mut peer_contexts = self.peer_contexts.write();
        peer_contexts.insert(
            id.clone(),
            PeerContexts {
                progress: init_progress,
            },
        );

        let thread_handles = ThreadHandles {
            replicator_handle: thread::replication::new(
                id.clone(),
                self.clone(),
                voter.clone(),
                self.queue_rx.clone(),
                self.replication_tx.clone(),
            ),
            heartbeater_handle: thread::heartbeat::new(id.clone(), voter),
        };
        self.peer_threads.lock().insert(id, thread_handles);

        Ok(())
    }

    fn remove_peer(&self, id: NodeId) {
        self.peer_threads.lock().remove(&id);
        self.peer_contexts.write().remove(&id);
    }

    pub async fn set_membership(
        &self,
        config: HashSet<NodeId>,
        index: Index,
        voter: Ref<Voter>,
    ) -> Result<()> {
        let cur = self.read_membership();

        let add_peers = {
            let mut out = vec![];
            for id in &config {
                if !cur.contains(id) {
                    out.push(id.clone());
                }
            }
            out
        };

        let remove_peers = {
            let mut out = vec![];
            for id in &cur {
                if !config.contains(id) {
                    out.push(id.clone());
                }
            }
            out
        };

        // $4.4
        // When making cluster membership changes that require multiple single-server steps,
        // it is preferable to add servers before removing servers.
        for id in add_peers {
            self.add_peer(id, voter.clone()).await?;
        }
        for id in remove_peers {
            self.remove_peer(id);
        }

        info!("membership changed -> {:?}", config);
        *self.membership.write() = config;

        self.command_log
            .membership_pointer
            .store(index, Ordering::SeqCst);

        Ok(())
    }

    pub fn read_membership(&self) -> HashSet<NodeId> {
        self.membership.read().clone()
    }

    pub async fn find_new_commit_index(&self) -> Result<Index> {
        let mut match_indices = vec![];

        let last_log_index = self.command_log.get_log_last_index().await?;
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

    pub fn reset_progress(&self, init_next_index: Index) {
        let mut peer_contexts = self.peer_contexts.write();
        for (_, peer) in peer_contexts.iter_mut() {
            peer.progress = ReplicationProgress::new(init_next_index);
        }
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
