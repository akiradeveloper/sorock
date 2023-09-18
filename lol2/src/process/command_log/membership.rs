use super::*;

impl CommandLog {
    /// Find the last last snapshot in [, to]
    pub async fn find_last_snapshot_index(&self, to: Index) -> Result<Option<Index>> {
        for i in (1..=to).rev() {
            let e = self.get_entry(i).await?;
            match Command::deserialize(&e.command) {
                Command::Snapshot { .. } => return Ok(Some(i)),
                _ => {}
            }
        }
        Ok(None)
    }

    /// Find the last membership change in [, to]
    pub async fn find_last_membership_index(&self, to: Index) -> Result<Option<Index>> {
        for i in (1..=to).rev() {
            let e = self.get_entry(i).await?;
            match Command::deserialize(&e.command) {
                Command::Snapshot { .. } => return Ok(Some(i)),
                Command::ClusterConfiguration { .. } => return Ok(Some(i)),
                _ => {}
            }
        }
        Ok(None)
    }

    pub async fn try_read_membership_change(
        &self,
        index: Index,
    ) -> Result<Option<HashSet<NodeId>>> {
        let e = self.get_entry(index).await?;
        match Command::deserialize(&e.command) {
            Command::Snapshot { membership } => Ok(Some(membership)),
            Command::ClusterConfiguration { membership } => Ok(Some(membership)),
            _ => Ok(None),
        }
    }
}
