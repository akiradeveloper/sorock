use super::*;

pub struct Effect {
    pub peers: Peers,
    pub command_log: CommandLog,
    pub voter: Read<Voter>,
    pub driver: RaftDriver,
}
impl Effect {
    async fn add_peer(&self, id: NodeId) -> Result<()> {
        if id == self.driver.self_node_id() {
            return Ok(());
        }

        if self.peers.peer_contexts.read().contains_key(&id) {
            return Ok(());
        }

        let init_progress = {
            let last_log_index = self.command_log.get_log_last_index().await?;
            ReplicationProgress::new(last_log_index)
        };

        let mut peer_contexts = self.peers.peer_contexts.write();
        peer_contexts.insert(
            id.clone(),
            PeerContexts {
                progress: init_progress,
            },
        );

        let thread_handles = ThreadHandles {
            replicator_handle: thread::replication::new(
                id.clone(),
                self.peers.clone(),
                self.voter.clone(),
                self.peers.queue_rx.clone(),
                self.peers.replication_tx.clone(),
            ),
            heartbeater_handle: thread::heartbeat::new(id.clone(), self.voter.clone()),
        };
        self.peers.peer_threads.lock().insert(id, thread_handles);

        Ok(())
    }

    fn remove_peer(&self, id: NodeId) {
        self.peers.peer_threads.lock().remove(&id);
        self.peers.peer_contexts.write().remove(&id);
    }

    pub async fn exec(self, config: HashSet<NodeId>, index: Index) -> Result<()> {
        let cur = self.peers.read_membership();

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
            self.add_peer(id).await?;
        }
        for id in remove_peers {
            self.remove_peer(id);
        }

        info!("membership changed -> {:?}", config);
        *self.peers.membership.write() = config;

        self.command_log
            .membership_pointer
            .store(index, Ordering::SeqCst);

        Ok(())
    }
}
