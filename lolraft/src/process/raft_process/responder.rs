use super::*;

impl RaftProcess {
    pub(crate) async fn send_replication_stream(
        &self,
        req: request::ReplicationStream,
    ) -> Result<response::ReplicationStream> {
        let n_inserted = self.queue_received_entries(req).await?;
        let resp = response::ReplicationStream {
            n_inserted,
            log_last_index: self.command_log.get_log_last_index().await?,
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
            ensure!(self.command_log.allow_queue_new_membership());
            self.queue_new_entry(
                Command::serialize(command),
                Completion::Kern(kern_completion),
            )
            .await?;

            rx.await?;
        } else {
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

        let resp = if std::matches!(
            self.voter.read_election_state(),
            voter::ElectionState::Leader
        ) {
            let (user_completion, rx) = completion::prepare_user_completion();

            let read_index = self.command_log.commit_pointer.load(Ordering::SeqCst);
            let query = query_queue::Query {
                message: req.message,
                user_completion,
            };
            self.query_tx.register(read_index, query)?;

            let app_index = self.command_log.user_pointer.load(Ordering::SeqCst);
            // This must be `>=` because it is possible both commit_index and app_index are updated.
            if app_index >= read_index {
                self.app_tx.push_event(thread::ApplicationEvent);
            }

            rx.await?
        } else {
            // This check is to avoid looping.
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
            // This check is to avoid looping.
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
        self.voter
            .receive_heartbeat(leader_id, term, leader_commit)
            .await?;
        Ok(())
    }

    pub(crate) async fn get_snapshot(&self, index: Index) -> Result<SnapshotStream> {
        let st = self.command_log.open_snapshot(index).await?;
        Ok(st)
    }

    pub(crate) async fn send_timeout_now(&self) -> Result<()> {
        info!("received TimeoutNow. try to become a leader.");
        self.voter.try_promote(true).await?;
        Ok(())
    }

    pub(crate) async fn request_vote(&self, req: request::RequestVote) -> Result<bool> {
        let candidate_term = req.vote_term;
        let candidate_id = req.candidate_id;
        let candidate_clock = req.candidate_clock;
        let force_vote = req.force_vote;
        let pre_vote = req.pre_vote;
        let vote_granted = self
            .voter
            .receive_vote_request(
                candidate_term,
                candidate_id,
                candidate_clock,
                force_vote,
                pre_vote,
            )
            .await?;
        Ok(vote_granted)
    }
}
