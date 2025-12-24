use super::*;

pub struct Effect<'a> {
    pub command_log: &'a mut CommandLog,
}

impl Effect<'_> {
    /// Advance the snapshot index if there is a newer snapshot proposed.
    pub async fn exec(self) -> Result<()> {
        let proposed_snapshot_index = self
            .command_log
            .app
            .read()
            .await
            .get_latest_snapshot()
            .await?;
        let cur_snapshot_index = self.command_log.snapshot_pointer;
        if proposed_snapshot_index > cur_snapshot_index {
            info!("found a newer proposed snapshot@{proposed_snapshot_index} > {cur_snapshot_index}. will move the snapshot index.");

            // Calculate membership at the new snapshot index
            let new_config = {
                let last_membership_index = self
                    .command_log
                    .find_last_membership_index(proposed_snapshot_index)
                    .await?
                    .context(Error::BadLogState)?;
                self.command_log
                    .try_read_membership(last_membership_index)
                    .await?
                    .context(Error::BadLogState)?
            };

            let new_snapshot_entry = {
                let old_entry = self.command_log.get_entry(proposed_snapshot_index).await?;
                Entry {
                    command: Command::serialize(Command::Snapshot {
                        membership: new_config,
                    }),
                    ..old_entry
                }
            };
            self.command_log.insert_snapshot(new_snapshot_entry).await?;
        }

        Ok(())
    }
}
