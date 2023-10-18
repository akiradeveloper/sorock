use super::*;

impl Voter {
    pub async fn receive_heartbeat(
        &self,
        leader_id: NodeId,
        leader_term: Term,
        leader_commit: Index,
    ) -> Result<()> {
        let _lk = self.vote_lock.lock().await;

        let mut ballot = self.read_ballot().await?;
        if leader_term < ballot.cur_term {
            warn!("heartbeat is stale. rejected");
            return Ok(());
        }

        self.leader_failure_detector
            .receive_heartbeat(leader_id.clone());

        if leader_term > ballot.cur_term {
            warn!("received heartbeat with newer term. reset ballot");
            ballot.cur_term = leader_term;
            ballot.voted_for = None;
            self.write_election_state(ElectionState::Follower);
        }

        if ballot.voted_for != Some(leader_id.clone()) {
            info!("learn the current leader ({leader_id})");
            ballot.voted_for = Some(leader_id);
        }

        self.write_ballot(ballot).await?;

        let new_commit_index =
            std::cmp::min(leader_commit, self.command_log.get_log_last_index().await?);
        self.command_log
            .commit_pointer
            .store(new_commit_index, Ordering::SeqCst);

        Ok(())
    }

    pub async fn send_heartbeat(&self, follower_id: NodeId) -> Result<()> {
        let ballot = self.read_ballot().await?;
        let leader_commit_index = self.command_log.commit_pointer.load(Ordering::SeqCst);
        let req = request::Heartbeat {
            leader_id: self.driver.selfid(),
            leader_term: ballot.cur_term,
            leader_commit_index,
        };
        let conn = self.driver.connect(follower_id);
        conn.send_heartbeat(req).await?;
        Ok(())
    }
}
