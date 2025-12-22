use super::*;

pub struct Thread {
    command_log_actor: Actor<CommandLog>,
}

impl Thread {
    async fn run_once(&self) -> Result<()> {
        command_log::effect::advance_snapshot::Effect {
            command_log: &mut *self.command_log_actor.write().await,
        }
        .exec()
        .await?;

        Ok(())
    }

    fn do_loop(self) -> ThreadHandle {
        let fut = async move {
            let mut interval = tokio::time::interval(Duration::from_secs(1));
            loop {
                interval.tick().await;
                self.run_once().await.ok();
            }
        };
        let hdl = tokio::spawn(fut).abort_handle();
        ThreadHandle(hdl)
    }
}

pub fn new(command_log: Actor<CommandLog>) -> ThreadHandle {
    Thread {
        command_log_actor: command_log,
    }
    .do_loop()
}
