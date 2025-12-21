use super::*;

impl Control {
    /// Restore the membership from the state of the log.
    pub async fn init(&mut self, ctrl_actor: Read<ControlActor>) -> Result<()> {
        let log_last_index = self.command_log.read().await.get_log_last_index().await?;

        // It is ensured that at least one configuration can be found.
        let last_config_index = self
            .command_log
            .read()
            .await
            .find_last_membership_index(log_last_index)
            .await?
            .context(Error::BadLogState)?;

        let last_membership = {
            let entry = self
                .command_log
                .read()
                .await
                .get_entry(last_config_index)
                .await?;
            match Command::deserialize(&entry.command) {
                Command::Snapshot { membership } => membership,
                Command::ClusterConfiguration { membership } => membership,
                _ => unreachable!(),
            }
        };

        effect::set_membership::Effect {
            ctrl: self,
            ctrl_actor,
        }
        .exec(last_membership, last_config_index)
        .await?;

        Ok(())
    }
}
