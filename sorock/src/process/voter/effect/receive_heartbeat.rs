use super::*;

pub struct Effect {
    pub voter: Voter,
    pub state_mechine: StateMachine,
}
impl Effect {
    pub async fn exec(
        self,
        leader_id: NodeId,
        leader_term: Term,
        leader_commit: Index,
    ) -> Result<()> {
        let _lk = self.voter.vote_lock.lock().await;

        let mut ballot = self.voter.read_ballot().await?;
        if leader_term < ballot.cur_term {
            warn!("heartbeat is stale. rejected");
            return Ok(());
        }

        self.voter
            .leader_failure_detector
            .receive_heartbeat(leader_id.clone());

        if leader_term > ballot.cur_term {
            warn!("received heartbeat with newer term. reset ballot");
            ballot.cur_term = leader_term;
            ballot.voted_for = None;
            self.voter.write_election_state(ElectionState::Follower);
        }

        if ballot.voted_for != Some(leader_id.clone()) {
            info!("learn the current leader ({leader_id})");
            ballot.voted_for = Some(leader_id);
        }

        self.voter.write_ballot(ballot).await?;

        let new_commit_index = std::cmp::min(
            leader_commit,
            self.state_mechine.get_log_last_index().await?,
        );
        self.state_mechine
            .commit_pointer
            .fetch_max(new_commit_index, Ordering::SeqCst);

        Ok(())
    }
}
