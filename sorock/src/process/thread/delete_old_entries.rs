use super::*;

#[derive(Clone)]
pub struct Thread {
    command_log: Read<Actor<CommandLog>>,
}
impl Thread {
    async fn run_once(&self) -> Result<()> {
        command_log::effect::delete_old_entries::Effect {
            command_log: &*self.command_log.read().await,
        }
        .exec()
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

pub fn new(command_log: Read<Actor<CommandLog>>) -> ThreadHandle {
    Thread { command_log }.do_loop()
}
