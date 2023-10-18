use super::*;

impl RaftProcess {
    pub async fn send_log_stream(&self, req: LogStream) -> Result<response::SendLogStream> {
        let success = self.queue_received_entry(req).await?;
        let resp = response::SendLogStream {
            success,
            log_last_index: self.command_log.get_log_last_index().await?,
        };
        Ok(resp)
    }

    pub async fn process_kern_request(&self, req: request::KernRequest) -> Result<()> {
        let ballot = self.voter.read_ballot().await.unwrap();

        ensure!(ballot.voted_for.is_some());
        let leader_id = ballot.voted_for.unwrap();

        if std::matches!(
            self.voter.read_election_state(),
            voter::ElectionState::Leader
        ) {
            let (kern_completion, rx) = completion::prepare_kern_completion();
            let command = match kern_message::KernRequest::deserialize(&req.message).unwrap() {
                kern_message::KernRequest::AddServer(id) => {
                    ensure!(self.command_log.allow_queue_new_membership());

                    let mut membership = self.peers.read_membership();
                    membership.insert(id);
                    Command::ClusterConfiguration { membership }
                }
                kern_message::KernRequest::RemoveServer(id) => {
                    ensure!(self.command_log.allow_queue_new_membership());

                    let mut membership = self.peers.read_membership();
                    membership.remove(&id);
                    Command::ClusterConfiguration { membership }
                }
            };
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

    pub async fn process_user_read_request(&self, req: request::UserReadRequest) -> Result<Bytes> {
        let ballot = self.voter.read_ballot().await.unwrap();

        ensure!(ballot.voted_for.is_some());
        let leader_id = ballot.voted_for.unwrap();

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
            self.query_queue.register(read_index, query).await;

            rx.await?
        } else {
            // This check is to avoid looping.
            ensure!(self.driver.selfid() != leader_id);
            let conn = self.driver.connect(leader_id);
            conn.process_user_read_request(req).await?
        };
        Ok(resp)
    }

    pub async fn process_user_write_request(
        &self,
        req: request::UserWriteRequest,
    ) -> Result<Bytes> {
        let ballot = self.voter.read_ballot().await.unwrap();

        ensure!(ballot.voted_for.is_some());
        let leader_id = ballot.voted_for.unwrap();

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
            ensure!(self.driver.selfid() != leader_id);

            let conn = self.driver.connect(leader_id);
            conn.process_user_write_request(req).await?
        };
        Ok(resp)
    }

    pub async fn send_heartbeat(&self, req: request::Heartbeat) -> Result<()> {
        let leader_id = req.leader_id;
        let term = req.leader_term;
        let leader_commit = req.leader_commit_index;
        self.voter
            .receive_heartbeat(leader_id, term, leader_commit)
            .await?;
        Ok(())
    }

    pub async fn get_snapshot(&self, index: Index) -> Result<SnapshotStream> {
        let st = self.command_log.open_snapshot(index).await?;
        Ok(st)
    }

    pub async fn send_timeout_now(&self) -> Result<()> {
        info!("received TimeoutNow. try to become a leader.");
        self.voter.try_promote(true).await?;
        Ok(())
    }

    pub async fn request_vote(&self, req: request::RequestVote) -> Result<bool> {
        let candidate_term = req.vote_term;
        let candidate_id = req.candidate_id;
        let candidate_clock = req.candidate_clock;
        let force_vote = req.force_vote;
        let pre_vote = req.pre_vote;
        let vote_granted = self
            .voter
            .receive_vote(
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
