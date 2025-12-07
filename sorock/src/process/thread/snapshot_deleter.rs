use super::*;

#[derive(Clone)]
pub struct Thread {
    state_mechine: StateMachine,
    app: App,
}
impl Thread {
    async fn run_once(&self) -> Result<()> {
        self.state_mechine
            .delete_old_snapshots(self.app.clone())
            .await
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

pub fn new(state_mechine: StateMachine, app: App) -> ThreadHandle {
    Thread { state_mechine, app }.do_loop()
}
