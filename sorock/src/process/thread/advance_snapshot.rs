use super::*;

#[derive(Clone)]
pub struct Thread {
    command_log: CommandLog,
    app: App,
}
impl Thread {
    async fn run_once(&self) -> Result<()> {
        command_log::effect::advance_snapshot_index::Effect {
            command_log: self.command_log.clone(),
            app: self.app.clone(),
        }.exec().await?;

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

pub fn new(command_log: CommandLog, app: App) -> ThreadHandle {
    Thread { command_log, app }.do_loop()
}
