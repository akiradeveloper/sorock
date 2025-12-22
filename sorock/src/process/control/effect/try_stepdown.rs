use super::*;

pub struct Effect<'a> {
    pub ctrl: &'a mut Control,
}
impl Effect<'_> {
    fn command_log(&self) -> &Read<Actor<CommandLog>> {
        &self.ctrl.command_log
    }

    /// If the latest config doesn't contain itself, then it steps down
    /// by transferring the leadership to another node.
    pub async fn exec(self) -> Result<()> {
        ensure!(std::matches!(
            self.ctrl.read_election_state(),
            control::ElectionState::Leader
        ));

        // Make sure the membership entry is truly committed
        // otherwise the configuration change entry may be lost.
        let last_membership_change_index = {
            let index = self.ctrl.membership_pointer;
            ensure!(index <= self.ctrl.commit_pointer);
            index
        };

        let config = self
            .command_log()
            .read()
            .await
            .try_read_membership(last_membership_change_index)
            .await?
            .context(Error::BadLogState)?;
        ensure!(!config.contains(&self.ctrl.driver.self_node_id()));

        info!("step down");
        self.ctrl
            .write_election_state(control::ElectionState::Follower);
        self.ctrl.transfer_leadership().await?;

        Ok(())
    }
}
