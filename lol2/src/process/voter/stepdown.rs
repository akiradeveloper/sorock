use super::*;

impl Voter {
    /// If the latest config doesn't contain itself, then it steps down
    /// by transferring the leadership to another node.
    pub async fn try_stepdown(&self) -> Result<()> {
        ensure!(std::matches!(
            self.read_election_state(),
            voter::ElectionState::Leader
        ));

        let last_membership_change_index =
            self.command_log.membership_pointer.load(Ordering::SeqCst);
        // Ensure the membership entry is committed otherwise add-server request may be lost.
        ensure!(
            last_membership_change_index <= self.command_log.commit_pointer.load(Ordering::SeqCst)
        );

        let config = self
            .command_log
            .try_read_membership(last_membership_change_index)
            .await?
            .context(Error::BadLogState)?;
        ensure!(!config.contains(&self.driver.self_node_id()));

        info!("step down");
        self.write_election_state(voter::ElectionState::Follower);
        self.peers.transfer_leadership().await?;

        Ok(())
    }
}
