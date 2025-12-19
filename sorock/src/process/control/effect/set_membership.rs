use super::*;

pub struct Effect {
    pub ctrl: Control,
}
impl Effect {
    fn state_machine(&self) -> &Read<StateMachine> {
        &self.ctrl.state_machine
    }

    async fn add_peer(&self, id: NodeAddress) -> Result<()> {
        if id == self.ctrl.driver.self_node_id() {
            return Ok(());
        }

        if self.ctrl.replication_progresses.read().contains_key(&id) {
            return Ok(());
        }

        let init_progress = {
            let last_log_index = self.state_machine().get_log_last_index().await?;
            ReplicationProgress::new(last_log_index)
        };

        let mut replication_progresses = self.ctrl.replication_progresses.write();
        replication_progresses.insert(id.clone(), init_progress);

        let thread_handles = ThreadHandles {
            replicator_handle: thread::replication::new(
                id.clone(),
                self.ctrl.clone(),
                self.ctrl.queue_rx.clone(),
                self.ctrl.replication_tx.clone(),
            ),
            heartbeater_handle: thread::heartbeat::new(id.clone(), Read(self.ctrl.clone())),
        };
        self.ctrl.peer_threads.lock().insert(id, thread_handles);

        Ok(())
    }

    fn remove_peer(&self, id: NodeAddress) {
        self.ctrl.peer_threads.lock().remove(&id);
        self.ctrl.replication_progresses.write().remove(&id);
    }

    pub async fn exec(self, config: HashSet<NodeAddress>, index: LogIndex) -> Result<()> {
        let cur = self.ctrl.read_membership();

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
        *self.ctrl.membership.write() = config;

        self.ctrl.membership_pointer.store(index, Ordering::SeqCst);

        Ok(())
    }
}
