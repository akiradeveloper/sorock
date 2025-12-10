use super::*;

#[derive(Clone)]
pub struct Thread {
    ctrl: Control,
    state_machine: StateMachine,
}
impl Thread {
    async fn run_once(&self) -> Result<()> {
        let election_state = self.ctrl.read_election_state();
        ensure!(std::matches!(
            election_state,
            control::ElectionState::Follower
        ));

        // sleep random duration
        if let Some(timeout) = self.ctrl.get_election_timeout() {
            tokio::time::sleep(timeout).await;
        }
        // if it doesn't receive any heartbeat from a leader (or new leader)
        // it try to become a leader.
        if self.ctrl.get_election_timeout().is_some() {
            info!("election timeout. try to become a leader");
            control::effect::try_promote::Effect {
                ctrl: self.ctrl.clone(),
                state_machine: self.state_machine.clone(),
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

pub fn new(ctrl: Control, state_machine: StateMachine) -> ThreadHandle {
    Thread {
        ctrl,
        state_machine,
    }
    .do_loop()
}
