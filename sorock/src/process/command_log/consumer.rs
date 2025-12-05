use super::*;

impl CommandLog {
    /// Advance the snapshot index if there is a newer snapshot proposed.
    pub async fn advance_snapshot_index(&self, app: App) -> Result<()> {
        let mut g_snapshot_pointer = self.snapshot_pointer.write().await;

        let proposed_snapshot_index = app.get_latest_snapshot().await?;
        let cur_snapshot_index = *g_snapshot_pointer;
        if proposed_snapshot_index > cur_snapshot_index {
            info!("found a newer proposed snapshot@{proposed_snapshot_index} > {cur_snapshot_index}. will move the snapshot index.");

            // Calculate membership at the new snapshot index
            let new_config = {
                let last_membership_index = self
                    .find_last_membership_index(proposed_snapshot_index)
                    .await?
                    .context(Error::BadLogState)?;
                self.try_read_membership(last_membership_index)
                    .await?
                    .context(Error::BadLogState)?
            };

            let new_snapshot_entry = {
                let old_entry = self.get_entry(proposed_snapshot_index).await?;
                Entry {
                    command: Command::serialize(Command::Snapshot {
                        membership: new_config,
                    }),
                    ..old_entry
                }
            };

            self.insert_snapshot(new_snapshot_entry).await?;
            *g_snapshot_pointer = proposed_snapshot_index;
        }

        Ok(())
    }

    /// Advance kernel process once.
    pub(crate) async fn advance_kern_process(&self, voter: Voter) -> Result<()> {
        let cur_kern_index = self.kern_pointer.load(Ordering::SeqCst);
        ensure!(cur_kern_index < self.commit_pointer.load(Ordering::SeqCst));

        let process_index = cur_kern_index + 1;
        let e = self.get_entry(process_index).await?;
        let command = Command::deserialize(&e.command);

        let do_process = match command {
            Command::Barrier { .. } => true,
            Command::ClusterConfiguration { .. } => true,
            _ => false,
        };

        if do_process {
            debug!("process kern@{process_index}");
            match command {
                Command::Barrier(term) => {
                    voter.commit_safe_term(term);
                }
                Command::ClusterConfiguration { .. } => {}
                _ => {}
            }
            if let Some(kern_completion) = self.kern_completions.lock().remove(&process_index) {
                kern_completion.complete();
            }
        }

        self.kern_pointer.fetch_max(process_index, Ordering::SeqCst);

        Ok(())
    }
}
