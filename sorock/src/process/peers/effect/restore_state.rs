use super::*;

pub struct Effect {
    pub peers: Peers,
    pub command_log: CommandLog,
    pub voter: Read<Voter>,
    pub driver: RaftDriver,
}

impl Effect {
    /// Restore the membership from the state of the log.
    pub async fn exec(self) -> Result<()> {
        let log_last_index = self.command_log.get_log_last_index().await?;
        let last_membership_index = self
            .command_log
            .find_last_membership_index(log_last_index)
            .await?;

        if let Some(last_membership_index) = last_membership_index {
            let last_membership = {
                let entry = self.command_log.get_entry(last_membership_index).await?;
                match Command::deserialize(&entry.command) {
                    Command::Snapshot { membership } => membership,
                    Command::ClusterConfiguration { membership } => membership,
                    _ => unreachable!(),
                }
            };

            effect::set_membership::Effect {
                peers: self.peers.clone(),
                command_log: self.command_log.clone(),
                voter: self.voter.clone(),
                driver: self.driver.clone(),
            }
            .exec(last_membership, last_membership_index)
            .await?;
        };

        Ok(())
    }
}
