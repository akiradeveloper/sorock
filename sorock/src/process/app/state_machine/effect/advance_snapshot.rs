use super::*;

pub struct Effect {
    pub state_machine: StateMachine,
}

impl Effect {
    /// Advance the snapshot index if there is a newer snapshot proposed.
    pub async fn exec(&self) -> Result<()> {
        let _g = self.state_machine.write_sequencer.try_acquire()?;

        let proposed_snapshot_index = self.state_machine.app.get_latest_snapshot().await?;
        let cur_snapshot_index = self.state_machine.snapshot_pointer.load(Ordering::SeqCst);
        if proposed_snapshot_index > cur_snapshot_index {
            info!("found a newer proposed snapshot@{proposed_snapshot_index} > {cur_snapshot_index}. will move the snapshot index.");

            // Calculate membership at the new snapshot index
            let new_config = {
                let last_membership_index = self
                    .state_machine
                    .find_last_membership_index(proposed_snapshot_index)
                    .await?
                    .context(Error::BadLogState)?;
                self.state_machine
                    .try_read_membership(last_membership_index)
                    .await?
                    .context(Error::BadLogState)?
            };

            let new_snapshot_entry = {
                let old_entry = self
                    .state_machine
                    .get_entry(proposed_snapshot_index)
                    .await?;
                Entry {
                    command: Command::serialize(Command::Snapshot {
                        membership: new_config,
                    }),
                    ..old_entry
                }
            };

            self.state_machine
                .insert_snapshot(new_snapshot_entry)
                .await?;
            self.state_machine
                .snapshot_pointer
                .store(proposed_snapshot_index, Ordering::SeqCst);
        }

        Ok(())
    }
}
