use super::*;

#[allow(dead_code)]
struct ThreadHandles {
    advance_kern_handle: thread::ThreadHandle,
    advance_user_handle: thread::ThreadHandle,
    advance_snapshot_handle: thread::ThreadHandle,
    advance_commit_handle: thread::ThreadHandle,
    election_handle: thread::ThreadHandle,
    log_compaction_handle: thread::ThreadHandle,
    query_execution_handle: thread::ThreadHandle,
    snapshot_deleter_handle: thread::ThreadHandle,
    stepdown_handle: thread::ThreadHandle,
}

/// `RaftProcess` is a implementation of Raft process in `RaftNode`.
/// `RaftProcess` is unaware of the gRPC and the network but just focuses on the Raft algorithm.
pub struct RaftProcess {
    state_mechine: StateMachine,
    voter: Voter,
    peers: Peers,
    query_queue: query_processor::QueryQueue,
    app: App,
    driver: RaftDriver,
    _thread_handles: ThreadHandles,

    queue_tx: thread::EventProducer<thread::QueueEvent>,
    replication_tx: thread::EventProducer<thread::ReplicationEvent>,
}

impl RaftProcess {
    pub async fn new(
        app: impl RaftApp,
        log_store: impl RaftLogStore,
        ballot_store: impl RaftBallotStore,
        driver: RaftDriver,
    ) -> Result<Self> {
        let app = App::new(app);

        let (query_tx, query_rx) = query_processor::new(Read(app.clone()));

        let state_mechine = StateMachine::new(log_store, app.clone());
        state_machine::effect::restore_state::Effect {
            state_mechine: state_mechine.clone(),
        }
        .exec()
        .await?;

        let (queue_tx, queue_rx) = thread::notify();
        let (replication_tx, replication_rx) = thread::notify();
        let (commit_tx, commit_rx) = thread::notify();
        let (kern_tx, kern_rx) = thread::notify();
        let (app_tx, app_rx) = thread::notify();

        let peers = Peers::new(
            Read(state_mechine.clone()),
            queue_rx.clone(),
            replication_tx.clone(),
            driver.clone(),
        );

        let voter = Voter::new(
            ballot_store,
            Read(state_mechine.clone()),
            Read(peers.clone()),
            driver.clone(),
        );

        peers::effect::restore_state::Effect {
            peers: peers.clone(),
            state_mechine: state_mechine.clone(),
            voter: Read(voter.clone()),
            driver: driver.clone(),
        }
        .exec()
        .await?;

        let _thread_handles = ThreadHandles {
            advance_kern_handle: thread::advance_kern::new(
                state_mechine.clone(),
                voter.clone(),
                commit_rx.clone(),
                kern_tx.clone(),
            ),
            advance_user_handle: thread::advance_user::new(
                state_mechine.clone(),
                kern_rx.clone(),
                app_tx.clone(),
            ),
            advance_snapshot_handle: thread::advance_snapshot::new(state_mechine.clone()),
            advance_commit_handle: thread::advance_commit::new(
                state_mechine.clone(),
                Read(peers.clone()),
                Read(voter.clone()),
                replication_rx.clone(),
                commit_tx.clone(),
            ),
            election_handle: thread::election::new(
                voter.clone(),
                state_mechine.clone(),
                peers.clone(),
            ),
            log_compaction_handle: thread::log_compaction::new(state_mechine.clone()),
            query_execution_handle: thread::query_execution::new(
                query_rx.clone(),
                Read(state_mechine.clone()),
                app_rx.clone(),
            ),
            snapshot_deleter_handle: thread::snapshot_deleter::new(
                state_mechine.clone(),
                app.clone(),
            ),
            stepdown_handle: thread::stepdown::new(
                voter.clone(),
                Read(state_mechine.clone()),
                Read(peers.clone()),
            ),
        };

        Ok(Self {
            state_mechine,
            voter,
            peers,
            query_queue: query_tx,
            driver,
            app,
            _thread_handles,

            queue_tx,
            replication_tx,
        })
    }

    /// Process configuration change if the command contains configuration.
    /// Configuration should be applied as soon as it is inserted into the log because doing so
    /// guarantees that majority of the servers move to the configuration when the entry is committed.
    /// Without this property, servers may still be in some old configuration which may cause split-brain
    /// by electing two leaders in a single term which is not allowed in Raft.
    async fn process_configuration_command(&self, command: &[u8], index: Index) -> Result<()> {
        let config0 = match Command::deserialize(command) {
            Command::Snapshot { membership } => Some(membership),
            Command::ClusterConfiguration { membership } => Some(membership),
            _ => None,
        };
        if let Some(config) = config0 {
            peers::effect::set_membership::Effect {
                peers: self.peers.clone(),
                state_mechine: self.state_mechine.clone(),
                voter: Read(self.voter.clone()),
                driver: self.driver.clone(),
            }
            .exec(config, index)
            .await?;
        }
        Ok(())
    }

    async fn queue_new_entry(&self, command: Bytes, completion: Completion) -> Result<Index> {
        ensure!(self.voter.allow_queue_new_entry().await?);

        let append_index = state_machine::effect::append_new_entry::Effect {
            state_mechine: self.state_mechine.clone(),
        }
        .exec(command.clone(), None)
        .await?;

        self.state_mechine
            .register_completion(append_index, completion);

        self.process_configuration_command(&command, append_index)
            .await?;

        self.queue_tx.push_event(thread::QueueEvent);
        self.replication_tx.push_event(thread::ReplicationEvent);

        Ok(append_index)
    }

    async fn queue_received_entries(&self, mut req: request::ReplicationStream) -> Result<u64> {
        let mut prev_clock = req.prev_clock;
        let mut n_inserted = 0;
        while let Some(Some(cur)) = req.entries.next().await {
            let entry = Entry {
                prev_clock,
                this_clock: cur.this_clock,
                command: cur.command,
            };
            let insert_index = entry.this_clock.index;
            let command = entry.command.clone();

            use state_machine::effect::try_insert::TryInsertResult;

            let insert_result = state_machine::effect::try_insert::Effect {
                state_mechine: self.state_mechine.clone(),
                driver: self.driver.clone(),
            }
            .exec(entry, req.sender_id.clone())
            .await?;

            match insert_result {
                TryInsertResult::Inserted => {
                    self.process_configuration_command(&command, insert_index)
                        .await?;
                }
                TryInsertResult::SkippedInsertion => {}
                TryInsertResult::InconsistentInsertion { want, found } => {
                    warn!("rejected append entry (clock={:?}) for inconsisntency (want:{want:?} != found:{found:?}", cur.this_clock);
                    break;
                }
                TryInsertResult::LeapInsertion { want } => {
                    debug!(
                        "rejected append entry (clock={:?}) for leap insertion (want={want:?})",
                        cur.this_clock
                    );
                    break;
                }
            }
            prev_clock = cur.this_clock;
            n_inserted += 1;
        }

        Ok(n_inserted)
    }

    /// Forming a new cluster with a single node is called "cluster bootstrapping".
    /// Raft algorith doesn't define adding node when the cluster is empty.
    /// We need to handle this special case.
    async fn bootstrap_cluster(&self) -> Result<()> {
        let mut membership = HashSet::new();
        membership.insert(self.driver.self_node_id());

        let command = Command::serialize(Command::ClusterConfiguration { membership });
        let config = Entry {
            prev_clock: Clock { term: 0, index: 1 },
            this_clock: Clock { term: 0, index: 2 },
            command: command.clone(),
        };
        self.state_mechine.insert_entry(config).await?;

        self.process_configuration_command(&command, 2).await?;

        // After this function is called
        // this server should immediately become the leader by self-vote and advance commit index.
        // Consequently, when initial install_snapshot is called this server is already the leader.
        let conn = self.driver.connect(self.driver.self_node_id());
        conn.send_timeout_now().await?;

        Ok(())
    }

    pub(crate) async fn add_server(&self, req: request::AddServer) -> Result<()> {
        if self.peers.read_membership().is_empty() && req.server_id == self.driver.self_node_id() {
            self.bootstrap_cluster().await?;
        } else {
            let msg = kern_message::KernRequest::AddServer(req.server_id);
            let req = request::KernRequest {
                message: msg.serialize(),
            };
            let conn = self.driver.connect(self.driver.self_node_id());
            conn.process_kern_request(req).await?;
        }
        Ok(())
    }

    pub(crate) async fn remove_server(&self, req: request::RemoveServer) -> Result<()> {
        let msg = kern_message::KernRequest::RemoveServer(req.server_id);
        let req = request::KernRequest {
            message: msg.serialize(),
        };
        let conn = self.driver.connect(self.driver.self_node_id());
        conn.process_kern_request(req).await?;
        Ok(())
    }

    pub(crate) async fn send_replication_stream(
        &self,
        req: request::ReplicationStream,
    ) -> Result<response::ReplicationStream> {
        let n_inserted = self.queue_received_entries(req).await?;

        let resp = response::ReplicationStream {
            n_inserted,
            log_last_index: self.state_mechine.get_log_last_index().await?,
        };
        Ok(resp)
    }

    pub(crate) async fn process_kern_request(&self, req: request::KernRequest) -> Result<()> {
        let ballot = self.voter.read_ballot().await?;

        let Some(leader_id) = ballot.voted_for else {
            bail!(Error::LeaderUnknown)
        };

        if std::matches!(
            self.voter.read_election_state(),
            voter::ElectionState::Leader
        ) {
            let (kern_completion, rx) = completion::prepare_kern_completion();
            let command = match kern_message::KernRequest::deserialize(&req.message).unwrap() {
                kern_message::KernRequest::AddServer(id) => {
                    let mut membership = self.peers.read_membership();
                    membership.insert(id);
                    Command::ClusterConfiguration { membership }
                }
                kern_message::KernRequest::RemoveServer(id) => {
                    let mut membership = self.peers.read_membership();
                    membership.remove(&id);
                    Command::ClusterConfiguration { membership }
                }
            };
            ensure!(self.state_mechine.allow_queue_new_membership());
            self.queue_new_entry(
                Command::serialize(command),
                Completion::Kern(kern_completion),
            )
            .await?;

            rx.await?;
        } else {
            // Avoid looping.
            ensure!(self.driver.self_node_id() != leader_id);
            let conn = self.driver.connect(leader_id);
            conn.process_kern_request(req).await?;
        }
        Ok(())
    }

    pub(crate) async fn process_user_read_request(
        &self,
        req: request::UserReadRequest,
    ) -> Result<Bytes> {
        let ballot = self.voter.read_ballot().await?;

        let Some(leader_id) = ballot.voted_for else {
            anyhow::bail!(Error::LeaderUnknown)
        };

        let will_process = req.read_locally
            || std::matches!(
                self.voter.read_election_state(),
                voter::ElectionState::Leader
            );

        let resp = if will_process {
            let (user_completion, rx) = completion::prepare_user_completion();

            let read_index = self.state_mechine.commit_pointer.load(Ordering::SeqCst);
            let query = query_processor::Query {
                message: req.message,
                user_completion,
            };
            self.query_queue.register(read_index, query)?;

            rx.await?
        } else {
            // Avoid looping.
            ensure!(self.driver.self_node_id() != leader_id);
            let conn = self.driver.connect(leader_id);
            conn.process_user_read_request(req).await?
        };
        Ok(resp)
    }

    pub(crate) async fn process_user_write_request(
        &self,
        req: request::UserWriteRequest,
    ) -> Result<Bytes> {
        let ballot = self.voter.read_ballot().await?;

        let Some(leader_id) = ballot.voted_for else {
            bail!(Error::LeaderUnknown)
        };

        let resp = if std::matches!(
            self.voter.read_election_state(),
            voter::ElectionState::Leader
        ) {
            let (user_completion, rx) = completion::prepare_user_completion();

            let command = Command::ExecuteRequest {
                message: &req.message,
                request_id: req.request_id,
            };

            self.queue_new_entry(
                Command::serialize(command),
                Completion::User(user_completion),
            )
            .await?;

            rx.await?
        } else {
            // Avoid looping.
            ensure!(self.driver.self_node_id() != leader_id);
            let conn = self.driver.connect(leader_id);
            conn.process_user_write_request(req).await?
        };
        Ok(resp)
    }

    pub(crate) async fn receive_heartbeat(
        &self,
        leader_id: NodeId,
        req: request::Heartbeat,
    ) -> Result<()> {
        let term = req.leader_term;
        let leader_commit = req.leader_commit_index;

        voter::effect::receive_heartbeat::Effect {
            voter: self.voter.clone(),
            state_mechine: self.state_mechine.clone(),
        }
        .exec(leader_id, term, leader_commit)
        .await?;

        Ok(())
    }

    pub(crate) async fn get_snapshot(&self, index: Index) -> Result<SnapshotStream> {
        let st = self
            .state_mechine
            .open_snapshot(index, self.app.clone())
            .await?;
        Ok(st)
    }

    pub(crate) async fn send_timeout_now(&self) -> Result<()> {
        info!("received TimeoutNow. try to become a leader.");
        voter::effect::try_promote::Effect {
            voter: self.voter.clone(),
            state_mechine: self.state_mechine.clone(),
            peers: self.peers.clone(),
        }
        .exec(true)
        .await?;
        Ok(())
    }

    pub(crate) async fn request_vote(&self, req: request::RequestVote) -> Result<bool> {
        let candidate_term = req.vote_term;
        let candidate_id = req.candidate_id;
        let candidate_clock = req.candidate_clock;
        let force_vote = req.force_vote;
        let pre_vote = req.pre_vote;

        let vote_granted = voter::effect::receive_vote_request::Effect {
            voter: self.voter.clone(),
            // state_mechine: Read(self.state_mechine.clone()),
        }
        .exec(
            candidate_term,
            candidate_id,
            candidate_clock,
            force_vote,
            pre_vote,
        )
        .await?;

        Ok(vote_granted)
    }

    pub(crate) async fn get_log_state(&self) -> Result<response::LogState> {
        let out = response::LogState {
            head_index: self.state_mechine.get_log_head_index().await?,
            last_index: self.state_mechine.get_log_last_index().await?,
            snap_index: self.state_mechine.get_snapshot_index().await,
            app_index: self.state_mechine.user_pointer.load(Ordering::SeqCst),
            commit_index: self.state_mechine.commit_pointer.load(Ordering::SeqCst),
        };
        Ok(out)
    }

    pub(crate) async fn get_membership(&self) -> Result<response::Membership> {
        let out = response::Membership {
            members: self.peers.read_membership(),
        };
        Ok(out)
    }
}
