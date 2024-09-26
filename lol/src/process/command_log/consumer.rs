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

    /// Advance the snapshot index if there is a newer snapshot proposed.
    pub async fn advance_snapshot_index(&self) -> Result<()> {
        let cur_snapshot_index = self.snapshot_pointer.load(Ordering::SeqCst);
        let proposed_snapshot_index = self.app.get_latest_snapshot().await?;
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
        }

        Ok(())
    }

    /// Advance user process once.
    pub(crate) async fn advance_user_process(&self, app: App) -> Result<()> {
        let cur_user_index = self.user_pointer.load(Ordering::SeqCst);
        ensure!(cur_user_index < self.kern_pointer.load(Ordering::SeqCst));

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
            let mut response_cache = self.response_cache.lock();
            debug!("process user@{process_index}");
            match command {
                Command::Snapshot { .. } => {
                    app.apply_snapshot(process_index).await?;
                }
                Command::ExecuteRequest {
                    message,
                    request_id,
                } => {
                    if response_cache.should_execute(&request_id) {
                        let resp = app.process_write(message, process_index).await?;
                        response_cache.insert_response(request_id.clone(), resp);
                    }

                    // Leader may have the completion for the request.
                    let user_completion = self.user_completions.lock().remove(&process_index);
                    if let Some(user_completion) = user_completion {
                        if let Some(resp) = response_cache.get_response(&request_id) {
                            // If client abort the request before retry,
                            // the completion channel is destroyed because the gRPC is context is cancelled.
                            // In this case, we should keep the response in the cache for the later request.
                            if let Err(resp) = user_completion.complete_with(resp) {
                                response_cache.insert_response(request_id.clone(), resp);
                            } else {
                                // After the request is completed, we queue a `CompleteRequest` command for terminating the context.
                                // This should be queued and replicated to the followers.
                                // Otherwise followers will never know the request is completed and the context will never be terminated.
                                let command = Command::CompleteRequest { request_id };
                                self.append_new_entry(Command::serialize(command), None)
                                    .await
                                    .ok();
                            }
                        }
                    }
                }
                Command::CompleteRequest { request_id } => {
                    response_cache.complete_response(&request_id);
                }
                _ => {}
            }
        }

        self.user_pointer.store(process_index, Ordering::SeqCst);

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

        self.kern_pointer.store(process_index, Ordering::SeqCst);

        Ok(())
    }
}
