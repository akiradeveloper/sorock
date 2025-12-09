use super::*;

#[derive(Clone)]
pub struct Thread {
    state_machine: StateMachine,
}
impl Thread {
    async fn run_once(&self) -> Result<()> {
        state_machine::effect::advance_snapshot::Effect {
            state_machine: self.state_machine.clone(),
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

pub fn new(state_machine: StateMachine) -> ThreadHandle {
    Thread { state_machine }.do_loop()
}
