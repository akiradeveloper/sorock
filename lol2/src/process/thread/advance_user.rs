use super::*;

#[derive(Clone)]
pub struct Thread {
    command_log: CommandLog,
    app: App,
}

impl Thread {
    async fn advance_once(&self) -> Result<bool> {
        self.command_log
            .advance_user_process(self.app.clone())
            .await
    }

    fn do_loop(self) -> ThreadHandle {
        let hdl = tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_millis(100));
            loop {
                interval.tick().await;
                while let Ok(true) = self.advance_once().await {}
            }
        })
        .abort_handle();

        ThreadHandle(hdl)
    }
}

pub fn new(command_log: CommandLog, app: App) -> ThreadHandle {
    Thread { command_log, app }.do_loop()
}
