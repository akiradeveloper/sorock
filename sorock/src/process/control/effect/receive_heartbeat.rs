use super::*;

pub struct Effect<'a> {
    pub ctrl: &'a mut Control,
}
impl Effect<'_> {
    fn command_log(&self) -> &Read<Actor<CommandLog>> {
        &self.ctrl.command_log
    }

    pub async fn exec(
        self,
        leader_id: NodeAddress,
        leader_term: Term,
        leader_commit: LogIndex,
    ) -> Result<()> {
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
            self.command_log().read().await.get_log_last_index().await?,
        );
        self.ctrl.advance_commit_index(new_commit_index);

        Ok(())
    }
}
