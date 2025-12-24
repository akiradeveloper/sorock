use super::*;

pub struct Effect<'a> {
    pub ctrl: &'a mut Control,
}

impl Effect<'_> {
    /// If the latest config doesn't contain itself, then it steps down
    /// by transferring the leadership to another node.
    pub async fn exec(self) -> Result<()> {
        let is_leader = self.ctrl.is_leader();

        let local_is_voter = self.ctrl.is_voter(&self.ctrl.io.local_server_id);

        // If the local node is still a leader but not a voter any more, it should step down.
        if is_leader && !local_is_voter {
            info!("step down. transferring leadership to other node");
            self.ctrl
                .write_election_state(control::ElectionState::Follower);
            self.ctrl.transfer_leadership().await?;
        }

        Ok(())
    }
}
