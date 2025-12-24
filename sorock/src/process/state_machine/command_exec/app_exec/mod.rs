use super::*;

mod response_cache;
use response_cache::ResponseCache;

pub struct AppCommand {
    pub index: LogIndex,
    pub body: Option<AppCommandBody>,
}

pub struct AppCommandBody {
    pub command: Bytes,
    pub completion: Option<completion::AppCompletion>,
}

pub struct AppExec {
    app: Arc<App>,
    response_cache: ResponseCache,
    q: CommandWaitQueue<AppCommand>,
    command_log_actor: Actor<CommandLog>,
    pub applied_index: LogIndex,
}

impl AppExec {
    pub fn new(app: Arc<App>, command_log_actor: Actor<CommandLog>) -> Self {
        Self {
            app,
            response_cache: ResponseCache::new(),
            q: CommandWaitQueue::new(),
            command_log_actor,
            applied_index: 0,
        }
    }

    pub fn insert(&mut self, app_command: AppCommand) {
        self.q.insert(app_command.index, app_command);
    }

    pub async fn process_once(&mut self) -> bool {
        let Some((_, app_command)) = self.q.pop_next() else {
            return false;
        };

        match self.do_process_once(app_command).await {
            Ok(()) => true,
            Err(_) => false,
        }
    }

    async fn do_process_once(&mut self, app_command: AppCommand) -> Result<()> {
        let AppCommand { index, body } = app_command;

        let Some(body) = body else {
            // No need to process non-application commands.
            self.applied_index = index;
            return Ok(());
        };

        let command = Command::deserialize(&body.command);

        debug!("process app@{index}");
        match command {
            Command::Snapshot { .. } => {
                self.app.apply_snapshot(index).await?;
            }
            Command::ExecuteWriteRequest {
                message,
                request_id,
            } => {
                if self.response_cache.should_execute(&request_id) {
                    let resp = self.app.process_write(message, index).await?;
                    self.response_cache
                        .insert_response(request_id.clone(), resp);
                }

                if let Some(completion) = body.completion {
                    // Leader may have the completion for the request.
                    if let Some(resp) = self.response_cache.get_response(&request_id) {
                        // If client abort the request before retry,
                        // the completion channel is destroyed because the gRPC is context is cancelled.
                        // In this case, we should keep the response in the cache for the later request.
                        if let Err(resp) = completion.complete_with(resp) {
                            self.response_cache
                                .insert_response(request_id.clone(), resp);
                        } else {
                            // After the request is completed, we queue a `CompleteRequest` command for terminating the context.
                            // This should be queued and replicated to the followers.
                            // Otherwise followers will never know the request is completed and the context will never be terminated.
                            let command = Command::CompleteWriteRequest { request_id };
                            command_log::effect::append_entry::Effect {
                                command_log: &mut *self.command_log_actor.write().await,
                            }
                            .exec(Command::serialize(command), None, None)
                            .await
                            .ok();
                        }
                    }
                }
            }
            Command::CompleteWriteRequest { request_id } => {
                self.response_cache.complete_response(&request_id);
            }
            _ => {}
        }

        self.applied_index = index;
        Ok(())
    }
}
