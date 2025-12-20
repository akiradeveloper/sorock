use super::*;

pub struct Effect<'a> {
    pub command_log: &'a mut CommandLog,
}

impl Effect<'_> {
    pub async fn exec(self) -> Result<()> {
        let cur_app_index = self.command_log.application_pointer;
        ensure!(cur_app_index < self.command_log.kernel_pointer);

        let process_index = cur_app_index + 1;
        let e = self.command_log.get_entry(process_index).await?;
        let command = Command::deserialize(&e.command);

        let do_process = match command {
            Command::ExecuteRequest { .. } => true,
            Command::CompleteRequest { .. } => true,
            Command::Snapshot { .. } => true,
            _ => false,
        };

        if do_process {
            let response_cache = &mut self.command_log.response_cache;
            debug!("process user@{process_index}");
            match command {
                Command::Snapshot { .. } => {
                    self.command_log.app.apply_snapshot(process_index).await?;
                }
                Command::ExecuteRequest {
                    message,
                    request_id,
                } => {
                    if response_cache.should_execute(&request_id) {
                        let resp = self
                            .command_log
                            .app
                            .process_write(message, process_index)
                            .await?;
                        response_cache.insert_response(request_id.clone(), resp);
                    }

                    // Leader may have the completion for the request.
                    let app_completion = self
                        .command_log
                        .application_completions
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
                                command_log::effect::append_entry::Effect {
                                    command_log: self.command_log,
                                }
                                .exec(Command::serialize(command), None, None)
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

        self.command_log.application_pointer = process_index;

        Ok(())
    }
}
