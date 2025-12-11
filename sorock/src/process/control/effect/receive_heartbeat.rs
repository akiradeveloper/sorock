use super::*;

pub struct Effect {
    pub ctrl: Control,
}
impl Effect {
    fn state_machine(&self) -> &Read<StateMachine> {
        &self.ctrl.state_machine
    }

    pub async fn exec(
        self,
        leader_id: NodeAddress,
        leader_term: Term,
        leader_commit: LogIndex,
    ) -> Result<()> {
        let _g = self.ctrl.vote_sequencer.try_acquire()?;

        let mut ballot = self.ctrl.read_ballot().await?;
        if leader_term < ballot.cur_term {
            warn!("heartbeat is stale. rejected");
            return Ok(());
        }

        self.ctrl
            .leader_failure_detector
            .receive_heartbeat(leader_id.clone());

        if leader_term > ballot.cur_term {
            warn!("received heartbeat with newer term. reset ballot");
            ballot.cur_term = leader_term;
            ballot.voted_for = None;
            self.ctrl.write_election_state(ElectionState::Follower);
        }

        if ballot.voted_for != Some(leader_id.clone()) {
            info!("learn the current leader ({leader_id})");
            ballot.voted_for = Some(leader_id);
        }

        self.ctrl.write_ballot(ballot).await?;

        let new_commit_index = std::cmp::min(
            leader_commit,
            self.state_machine().get_log_last_index().await?,
        );
        self.ctrl
            .commit_pointer
            .fetch_max(new_commit_index, Ordering::SeqCst);

        Ok(())
    }
}
