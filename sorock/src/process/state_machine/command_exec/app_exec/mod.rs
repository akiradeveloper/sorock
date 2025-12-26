use super::*;

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
    response_cache: moka::sync::Cache<String, Bytes>,
    q: CommandWaitQueue<AppCommand>,
    command_log_actor: Actor<CommandLog>,
    pub applied_index: LogIndex,
}

impl AppExec {
    pub fn new(app: Arc<App>, command_log_actor: Actor<CommandLog>) -> Self {
        Self {
            app,
            response_cache: moka::sync::Cache::builder()
                // We don't care about requests that doesn't complete within 10 minutes.
                // Within this time, the client is able to retry the request sufficiently mulptiple times.
                .time_to_live(Duration::from_mins(10))
                .build(),
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
                // The section $6.3 describes a special case that clients may resend the same write request.
                // The author proposes to use session between clients and servers but I think it is overkill.
                // Instead, our solution is to use response cache with TTL.
                // This approach is much simpler but still effective to most of the practical use cases but
                // the worst-case senario leader stop occurs over and over again within TTL period, and that's
                // almost impossible in reality.

                // We don't execute the same request twice.
                if !self.response_cache.contains_key(&request_id) {
                    let resp = self.app.process_write(message, index).await?;
                    self.response_cache
                        .insert(request_id.clone(), resp.clone());
                }

                if let Some(completion) = body.completion {
                    if let Some(resp) = self.response_cache.get(&request_id) {
                        // If the completion succeeds, we assume the client has received the response.
                        if completion.complete_with(resp.clone()).is_ok() {
                            // After the request is completed, we queue a `CompleteWriteRequest` command terminating
                            // response caches from all nodes including followers.
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
                self.response_cache.invalidate(&request_id);
            }
            _ => {}
        }

        self.applied_index = index;

        Ok(())
    }
}
