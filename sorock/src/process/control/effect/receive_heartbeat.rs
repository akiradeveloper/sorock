use super::*;

pub struct Effect<'a> {
    pub ctrl: &'a mut Control,
}

impl Effect<'_> {
    fn command_log(&self) -> &Actor<CommandLog> {
        &self.ctrl.command_log_actor
    }

    pub async fn exec(
        self,
        sender_id: ServerAddress,
        sender_term: Term,
        sender_commit: LogIndex,
    ) -> Result<()> {
        let mut ballot = self.ctrl.read_ballot().await?;
        if sender_term < ballot.cur_term {
            warn!("heartbeat is stale. rejected");
            return Ok(());
        }

        self.ctrl
            .leader_failure_detector
            .receive_heartbeat(sender_id.clone());

        if sender_term > ballot.cur_term {
            warn!("received heartbeat with newer term. reset ballot");
            ballot.cur_term = sender_term;
            ballot.voted_for = None;
            self.ctrl.write_election_state(ElectionState::Follower);
        }

        if ballot.voted_for != Some(sender_id.clone()) {
            info!("learn the current leader ({sender_id})");
            ballot.voted_for = Some(sender_id);
        }

        self.ctrl.write_ballot(ballot).await?;

        let new_commit_index =
            std::cmp::min(sender_commit, self.command_log().read().await.tail_pointer);
        self.ctrl.advance_commit_index(new_commit_index);

        Ok(())
    }
}
