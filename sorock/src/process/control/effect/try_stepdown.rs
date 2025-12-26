use super::*;

pub struct Effect<'a> {
    pub ctrl: &'a mut Control,
}

impl Effect<'_> {
    /// If the latest config doesn't contain itself, then it steps down
    /// by transferring the leadership to another node.
    pub async fn exec(self) -> Result<()> {
        // $4.2.2
        // The configuration that removed this node should be committed before stepping down.
        if !self.ctrl.allow_queue_new_membership() {
            return Ok(());
        }

        let is_leader = self.ctrl.is_leader();
        let is_voter = self.ctrl.is_local_voter();

        // If the local node is still a leader but not a voter any more, it should step down.
        if is_leader && !is_voter {
            info!("step down. transferring leadership to other node");
            self.ctrl.write_election_state(ElectionState::Follower);
            self.ctrl.transfer_leadership().await?;
        }

        Ok(())
    }
}
