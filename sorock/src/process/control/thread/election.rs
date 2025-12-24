use super::*;

pub struct Thread {
    ctrl_actor: Actor<Control>,
    command_log: Actor<CommandLog>,
}

impl Thread {
    async fn run_once(&self) -> Result<()> {
        // Sleep random duration
        let timeout = self.ctrl_actor.read().await.get_election_timeout();
        if let Some(timeout) = timeout {
            tokio::time::sleep(timeout).await;
        } else {
            return Ok(());
        }

        // Double-check if it doesn't receive any heartbeat.
        let timeout = self.ctrl_actor.read().await.get_election_timeout();
        if timeout.is_some() {
            info!("election timeout. try to become a leader");
            control::effect::try_promote::Effect {
                ctrl: &mut *self.ctrl_actor.write().await,
                command_log: self.command_log.clone(),
            }
            .exec(false)
            .await?;
        }

        Ok(())
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

pub fn run(ctrl: Actor<Control>, command_log: Actor<CommandLog>) -> ThreadHandle {
    Thread {
        ctrl_actor: ctrl,
        command_log,
    }
    .run_loop()
}
