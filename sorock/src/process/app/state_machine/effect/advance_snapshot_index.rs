use super::*;

pub struct Effect {
    pub state_mechine: StateMachine,
}

impl Effect {
    /// Advance the snapshot index if there is a newer snapshot proposed.
    pub async fn exec(&self) -> Result<()> {
        let mut g_snapshot_pointer = self.state_mechine.snapshot_pointer.write().await;

        let proposed_snapshot_index = self.state_mechine.app.get_latest_snapshot().await?;
        let cur_snapshot_index = *g_snapshot_pointer;
        if proposed_snapshot_index > cur_snapshot_index {
            info!("found a newer proposed snapshot@{proposed_snapshot_index} > {cur_snapshot_index}. will move the snapshot index.");

            // Calculate membership at the new snapshot index
            let new_config = {
                let last_membership_index = self
                    .state_mechine
                    .find_last_membership_index(proposed_snapshot_index)
                    .await?
                    .context(Error::BadLogState)?;
                self.state_mechine
                    .try_read_membership(last_membership_index)
                    .await?
                    .context(Error::BadLogState)?
            };

            let new_snapshot_entry = {
                let old_entry = self
                    .state_mechine
                    .get_entry(proposed_snapshot_index)
                    .await?;
                Entry {
                    command: Command::serialize(Command::Snapshot {
                        membership: new_config,
                    }),
                    ..old_entry
                }
            };

            self.state_mechine
                .insert_snapshot(new_snapshot_entry)
                .await?;
            *g_snapshot_pointer = proposed_snapshot_index;
        }

        Ok(())
    }
}
