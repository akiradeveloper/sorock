use super::*;

pub struct Task {
    pub peers: PeerSvc,
    pub voter: Ref<Voter>,
}
impl Task {
    /// Process configuration change if the command contains configuration.
    /// Configuration should be applied as soon as it is inserted into the log because doing so
    /// guarantees that majority of the servers move to the configuration when the entry is committed.
    /// Without this property, servers may still be in some old configuration which may cause split-brain
    /// by electing two leaders in a single term which is not allowed in Raft.
    pub async fn exec(
        self,
        command: &[u8],
        index: Index,
    ) -> Result<()> {
        let config0 = match Command::deserialize(command) {
            Command::Snapshot { membership } => Some(membership),
            Command::ClusterConfiguration { membership } => Some(membership),
            _ => None,
        };
        if let Some(config) = config0 {
            self.peers
                .set_membership(config, index, self.voter.clone())
                .await?;
        }
        Ok(())
    }
}