use super::*;

impl CommandLog {
    pub fn register_completion(&self, index: Index, completion: Completion) {
        match completion {
            Completion::User(completion) => {
                self.user_completions.lock().insert(index, completion);
            }
            Completion::Kern(completion) => {
                self.kern_completions.lock().insert(index, completion);
            }
        }
    }

    pub async fn advance_snapshot_index(&self) -> Result<()> {
        let cur_snapshot_index = self.snapshot_pointer.load(Ordering::SeqCst);
        let proposed_snapshot_index = self.app.get_latest_snapshot().await?;
        if proposed_snapshot_index > cur_snapshot_index {
            info!("find a newer proposed snapshot@{proposed_snapshot_index}. will move the snapshot index.");

            // calculate membership at the new snapshot index
            let new_config = {
                let last_membership_index = self
                    .find_last_membership_index(proposed_snapshot_index)
                    .await?
                    .context(Error::LogStateError)?;
                self.try_read_membership_change(last_membership_index)
                    .await?
                    .context(Error::LogStateError)?
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
            // TODO wait for follower catch up
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
        let command = Command::deserialize(&e.command);

        let do_process = match command {
            Command::ExecuteRequest { .. } => true,
            Command::CompleteRequest { .. } => true,
            Command::Snapshot { .. } => true,
            _ => false,
        };

        if do_process {
            debug!("process user@{process_index}");
            match command {
                Command::Snapshot { .. } => {
                    app.install_snapshot(process_index).await?;
                }
                Command::ExecuteRequest {
                    message,
                    request_id,
                } => {
                    // If the request has never been executed, we should execute it.
                    if self.response_cache.should_execute(&request_id) {
                        let resp = app.process_write(message, process_index).await?;
                        self.response_cache
                            .insert_response(request_id.clone(), resp);
                    }

                    // Leader may have the completion for the request.
                    if let Some(user_completion) =
                        self.user_completions.lock().remove(&process_index)
                    {
                        if let Some(resp) = self.response_cache.get_response(&request_id) {
                            user_completion.complete_with(resp);
                            // After the request is completed, we queue a `CompleteRequest` command for terminating the context.
                            // This should be queued and replicated to the followers otherwise followers
                            // will never know the request is completed and the context will never be terminated.
                            let command = Command::CompleteRequest { request_id };
                            self.append_new_entry(Command::serialize(command), None)
                                .await
                                .ok();
                        }
                    }
                }
                Command::CompleteRequest { request_id } => {
                    self.response_cache.complete_response(&request_id);
                }
                _ => {}
            }
        }

        self.user_pointer.store(process_index, Ordering::SeqCst);

        Ok(true)
    }

    pub(crate) async fn advance_kern_process(&self, voter: Voter) -> Result<bool> {
        let cur_kern_index = self.kern_pointer.load(Ordering::SeqCst);
        if cur_kern_index >= self.commit_pointer.load(Ordering::SeqCst) {
            return Ok(false);
        }

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

        self.kern_pointer.store(process_index, Ordering::SeqCst);

        Ok(true)
    }
}
