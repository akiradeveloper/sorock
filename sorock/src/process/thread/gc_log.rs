use super::*;

#[derive(Clone)]
pub struct Thread {
    state_mechine: StateMachine,
}
impl Thread {
    async fn run_once(&self) -> Result<()> {
        self.state_mechine.delete_old_entries().await
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

pub fn new(state_mechine: StateMachine) -> ThreadHandle {
    Thread { state_mechine }.do_loop()
}
