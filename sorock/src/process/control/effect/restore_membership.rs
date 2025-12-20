use super::*;

pub struct Effect<'a> {
    pub ctrl_actor: Read<ControlActor>,
    pub ctrl: &'a mut Control,
}

impl Effect<'_> {
    fn command_log(&self) -> &Read<CommandLogActor> {
        &self.ctrl.command_log
    }

    /// Restore the membership from the state of the log.
    pub async fn exec(self) -> Result<()> {
        let log_last_index = self.command_log().read().await.get_log_last_index().await?;
        let last_membership_index = self
            .command_log()
            .read()
            .await
            .find_last_membership_index(log_last_index)
            .await?;

        if let Some(last_membership_index) = last_membership_index {
            let last_membership = {
                let entry = self
                    .command_log()
                    .read()
                    .await
                    .get_entry(last_membership_index)
                    .await?;
                match Command::deserialize(&entry.command) {
                    Command::Snapshot { membership } => membership,
                    Command::ClusterConfiguration { membership } => membership,
                    _ => unreachable!(),
                }
            };

            effect::set_membership::Effect {
                ctrl_actor: self.ctrl_actor.clone(),
                ctrl: self.ctrl,
            }
            .exec(last_membership, last_membership_index)
            .await?;
        };

        Ok(())
    }
}
