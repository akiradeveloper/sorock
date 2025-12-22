use super::*;

pub struct Thread {
    command_log_actor: Actor<CommandLog>,
    ctrl_actor: Actor<Control>,
    kernel_exec_actor: Actor<KernelExec>,
    commit_evt_rx: EventConsumer<CommitEvent>,
    kernel_queue_evt_tx: EventProducer<KernelQueueEvent>,
}

impl Thread {
    async fn advance_once(&self) -> Result<()> {
        let command0 = command_log::effect::advance_kernel_pointer::Effect {
            command_log: &mut *self.command_log_actor.write().await,
            ctrl_actor: self.ctrl_actor.clone(),
        }
        .exec()
        .await?;

        if let Some(command) = command0 {
            self.kernel_exec_actor.write().await.insert(command);
        }

        Ok(())
    }

    fn do_loop(self) -> ThreadHandle {
        let fut = async move {
            loop {
                self.commit_evt_rx
                    .consume_events(Duration::from_millis(100))
                    .await;
                while self.advance_once().await.is_ok() {
                    self.kernel_queue_evt_tx.push_event(KernelQueueEvent);
                }
            }
        };
        let hdl = tokio::spawn(fut).abort_handle();
        ThreadHandle(hdl)
    }
}

pub fn new(
    command_log_actor: Actor<CommandLog>,
    ctrl_actor: Actor<Control>,
    kernel_exec: Actor<KernelExec>,
    commit_evt_rx: EventConsumer<CommitEvent>,
    kernel_evt_tx: EventProducer<KernelQueueEvent>,
) -> ThreadHandle {
    Thread {
        command_log_actor,
        ctrl_actor,
        kernel_exec_actor: kernel_exec,
        commit_evt_rx,
        kernel_queue_evt_tx: kernel_evt_tx,
    }
    .do_loop()
}
