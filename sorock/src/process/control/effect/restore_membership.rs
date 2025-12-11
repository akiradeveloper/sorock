use super::*;

pub struct Effect {
    pub ctrl: Control,
    pub driver: RaftHandle,
}

impl Effect {
    fn state_machine(&self) -> &Read<StateMachine> {
        &self.ctrl.state_machine
    }

    /// Restore the membership from the state of the log.
    pub async fn exec(self) -> Result<()> {
        let log_last_index = self.state_machine().get_log_last_index().await?;
        let last_membership_index = self
            .state_machine()
            .find_last_membership_index(log_last_index)
            .await?;

        if let Some(last_membership_index) = last_membership_index {
            let last_membership = {
                let entry = self
                    .state_machine()
                    .get_entry(last_membership_index)
                    .await?;
                match Command::deserialize(&entry.command) {
                    Command::Snapshot { membership } => membership,
                    Command::ClusterConfiguration { membership } => membership,
                    _ => unreachable!(),
                }
            };

            effect::set_membership::Effect {
                ctrl: self.ctrl.clone(),
                driver: self.driver.clone(),
            }
            .exec(last_membership, last_membership_index)
            .await?;
        };

        Ok(())
    }
}
