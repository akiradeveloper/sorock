use super::*;

pub struct Effect {
    pub state_mechine: StateMachine,
}

impl Effect {
    pub async fn exec(self) -> Result<()> {
        let cur_app_index = self
            .state_mechine
            .application_pointer
            .load(Ordering::SeqCst);
        ensure!(cur_app_index < self.state_mechine.kernel_pointer.load(Ordering::SeqCst));

        let process_index = cur_app_index + 1;
        let e = self.state_mechine.get_entry(process_index).await?;
        let command = Command::deserialize(&e.command);

        let do_process = match command {
            Command::ExecuteRequest { .. } => true,
            Command::CompleteRequest { .. } => true,
            Command::Snapshot { .. } => true,
            _ => false,
        };

        if do_process {
            let mut response_cache = self.state_mechine.response_cache.lock();
            debug!("process user@{process_index}");
            match command {
                Command::Snapshot { .. } => {
                    self.state_mechine.app.apply_snapshot(process_index).await?;
                }
                Command::ExecuteRequest {
                    message,
                    request_id,
                } => {
                    if response_cache.should_execute(&request_id) {
                        let resp = self
                            .state_mechine
                            .app
                            .process_write(message, process_index)
                            .await?;
                        response_cache.insert_response(request_id.clone(), resp);
                    }

                    // Leader may have the completion for the request.
                    let app_completion = self
                        .state_mechine
                        .application_completions
                        .lock()
                        .remove(&process_index);
                    if let Some(app_completion) = app_completion {
                        if let Some(resp) = response_cache.get_response(&request_id) {
                            // If client abort the request before retry,
                            // the completion channel is destroyed because the gRPC is context is cancelled.
                            // In this case, we should keep the response in the cache for the later request.
                            if let Err(resp) = app_completion.complete_with(resp) {
                                response_cache.insert_response(request_id.clone(), resp);
                            } else {
                                // After the request is completed, we queue a `CompleteRequest` command for terminating the context.
                                // This should be queued and replicated to the followers.
                                // Otherwise followers will never know the request is completed and the context will never be terminated.
                                let command = Command::CompleteRequest { request_id };
                                state_machine::effect::append::Effect {
                                    state_mechine: self.state_mechine.clone(),
                                }
                                .exec(Command::serialize(command), None)
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

        self.state_mechine
            .application_pointer
            .fetch_max(process_index, Ordering::SeqCst);

        Ok(())
    }
}
