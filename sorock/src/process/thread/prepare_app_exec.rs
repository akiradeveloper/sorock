use super::*;

pub struct Thread {
    command_log_actor: Actor<CommandLog>,
    app_exec_actor: Actor<AppExec>,
    kernel_queue_evt_rx: EventConsumer<KernelQueueEvent>,
    app_queue_evt_tx: EventProducer<AppQueueEvent>,
}

impl Thread {
    async fn advance_once(&self) -> Result<()> {
        let command = command_log::effect::advance_app_pointer::Effect {
            command_log: &mut *self.command_log_actor.write().await,
        }
        .exec()
        .await?;

        self.app_exec_actor.write().await.insert(command);

        Ok(())
    }

    fn do_loop(self) -> ThreadHandle {
        let fut = async move {
            loop {
                self.kernel_queue_evt_rx
                    .consume_events(Duration::from_millis(100))
                    .await;
                while self.advance_once().await.is_ok() {
                    self.app_queue_evt_tx.push_event(AppQueueEvent);
                }
            }
        };
        let hdl = tokio::spawn(fut).abort_handle();
        ThreadHandle(hdl)
    }
}

pub fn new(
    command_log: Actor<CommandLog>,
    app_exec: Actor<AppExec>,
    kernel_evt_rx: EventConsumer<KernelQueueEvent>,
    app_evt_tx: EventProducer<AppQueueEvent>,
) -> ThreadHandle {
    Thread {
        command_log_actor: command_log,
        app_exec_actor: app_exec,
        kernel_queue_evt_rx: kernel_evt_rx,
        app_queue_evt_tx: app_evt_tx,
    }
    .do_loop()
}
