use super::*;

#[derive(Clone)]
pub struct Thread {
    ctrl: Actor<Control>,
    command_log: Actor<CommandLog>,
}
impl Thread {
    async fn run_once(&self) -> Result<()> {
        let election_state = self.ctrl.read().await.read_election_state();
        ensure!(std::matches!(
            election_state,
            control::ElectionState::Follower
        ));

        // sleep random duration
        let timeout = self.ctrl.read().await.get_election_timeout();
        if let Some(timeout) = timeout {
            tokio::time::sleep(timeout).await;
        }

        // if it doesn't receive any heartbeat from a leader (or new leader)
        // it try to become a leader.
        let timeout = self.ctrl.read().await.get_election_timeout();
        if timeout.is_some() {
            info!("election timeout. try to become a leader");
            control::effect::try_promote::Effect {
                ctrl: &mut *self.ctrl.write().await,
                command_log: self.command_log.clone(),
            }
            .exec(false)
            .await?;
        }

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

pub fn new(ctrl: Actor<Control>, command_log: Actor<CommandLog>) -> ThreadHandle {
    Thread { ctrl, command_log }.do_loop()
}
