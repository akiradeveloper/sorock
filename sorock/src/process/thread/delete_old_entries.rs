use super::*;

pub struct Thread {
    command_log_actor: Actor<CommandLog>,
}

impl Thread {
    async fn run_once(&self) -> Result<()> {
        command_log::effect::delete_old_entries::Effect {
            command_log: &*self.command_log_actor.read().await,
        }
        .exec()
        .await
    }

    fn run_loop(self) -> ThreadHandle {
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

pub fn run(command_log: Actor<CommandLog>) -> ThreadHandle {
    Thread {
        command_log_actor: command_log,
    }
    .run_loop()
}
