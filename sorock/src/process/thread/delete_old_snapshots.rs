use super::*;

#[derive(Clone)]
pub struct Thread {
    command_log: Read<CommandLogActor>,
    app: App,
}
impl Thread {
    async fn run_once(&self) -> Result<()> {
        let cur_snapshot_index = self.command_log.read().await.snapshot_pointer;
        self.app.delete_snapshots_before(cur_snapshot_index).await?;
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

pub fn new(app: App, command_log: Read<CommandLogActor>) -> ThreadHandle {
    Thread { command_log, app }.do_loop()
}
