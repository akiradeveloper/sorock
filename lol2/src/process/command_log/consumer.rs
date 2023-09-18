use super::*;

impl CommandLog {
    pub async fn advance_snapshot_index(&self) -> Result<()> {
        let cur_snapshot_index = self.snapshot_index.load(Ordering::SeqCst);
        let proposed_snapshot_index = self.app.propose_new_snapshot().await?;
        if proposed_snapshot_index > cur_snapshot_index {
            info!("find a newer proposed snapshot@{proposed_snapshot_index}. will move the snapshot index.");

            // calculate membership at the new snapshot index
            let new_config = {
                let last_membership_index = self
                    .find_last_membership_index(proposed_snapshot_index)
                    .await?
                    .unwrap();
                self.try_read_membership_change(last_membership_index)
                    .await?
                    .unwrap()
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
        }
        Ok(())
    }

    pub(crate) async fn advance_user_process(&self, app: App) -> Result<bool> {
        let cur_user_index = self.user_pointer.load(Ordering::SeqCst);
        if cur_user_index >= self.kern_pointer.load(Ordering::SeqCst) {
            return Ok(false);
        }

        let process_index = cur_user_index + 1;
        let e = self.get_entry(process_index).await?;

        debug!("process user@{process_index}");
        match Command::deserialize(&e.command) {
            Command::Snapshot { .. } => {
                app.install_snapshot(process_index).await?;
            }
            Command::Req { message } => {
                let resp = app.process_write(message, process_index).await?;
                if let Some(user_completion) =
                    self.user_completions.lock().unwrap().remove(&process_index)
                {
                    user_completion.complete_with(resp);
                }
            }
            _ => {}
        }

        self.user_pointer.fetch_max(process_index, Ordering::SeqCst);

        Ok(true)
    }

    pub(crate) async fn advance_kern_process(&self, voter: Voter) -> Result<bool> {
        let cur_kern_index = self.kern_pointer.load(Ordering::SeqCst);
        if cur_kern_index >= self.commit_index.load(Ordering::SeqCst) {
            return Ok(false);
        }

        let process_index = cur_kern_index + 1;
        let e = self.get_entry(process_index).await?;

        debug!("process kern@{process_index}");
        if std::matches!(Command::deserialize(&e.command), Command::Noop) {
            let term = e.this_clock.term;
            voter.commit_safe_term(term);
        }

        if let Some(kern_completion) = self.kern_completions.lock().unwrap().remove(&process_index)
        {
            kern_completion.complete();
        }

        self.kern_pointer.fetch_max(process_index, Ordering::SeqCst);

        Ok(true)
    }
}
