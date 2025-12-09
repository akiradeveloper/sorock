use super::*;

pub struct Effect {
    pub voter: Voter,
}
impl Effect {
    fn state_mechine(&self) -> &Read<StateMachine> {
        &self.voter.state_mechine
    }

    fn peers(&self) -> &Read<Peers> {
        &self.voter.peers
    }

    /// If the latest config doesn't contain itself, then it steps down
    /// by transferring the leadership to another node.
    pub async fn exec(self) -> Result<()> {
        ensure!(std::matches!(
            self.voter.read_election_state(),
            voter::ElectionState::Leader
        ));

        // Make sure the membership entry is truly committed
        // otherwise the configuration change entry may be lost.
        let last_membership_change_index = {
            let index = self
                .state_mechine()
                .membership_pointer
                .load(Ordering::SeqCst);
            ensure!(index <= self.state_mechine().commit_pointer.load(Ordering::SeqCst));
            index
        };

        let config = self
            .state_mechine()
            .try_read_membership(last_membership_change_index)
            .await?
            .context(Error::BadLogState)?;
        ensure!(!config.contains(&self.voter.driver.self_node_id()));

        info!("step down");
        self.voter
            .write_election_state(voter::ElectionState::Follower);
        self.peers().transfer_leadership().await?;

        Ok(())
    }
}
