use super::*;

#[derive(Clone)]
pub struct Thread {
    command_log: CommandLog,
}
impl Thread {
    async fn run_once(&self) -> Result<()> {
        self.command_log.delete_old_snapshots().await
    }

    fn do_loop(self) -> ThreadHandle {
        let fut = async move {
            let mut interval = tokio::time::interval(Duration::from_millis(1000));
            loop {
                interval.tick().await;
                self.run_once().await.ok();
            }
        };
        let hdl = tokio::spawn(fut).abort_handle();
        ThreadHandle(hdl)
    }
}

pub fn new(command_log: CommandLog) -> ThreadHandle {
    Thread { command_log }.do_loop()
}
