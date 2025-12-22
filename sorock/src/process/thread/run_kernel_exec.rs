use super::*;

pub struct Thread {
    pub kernel_exec_actor: Actor<KernelExec>,
    pub kernel_queue_evt_rx: EventConsumer<KernelQueueEvent>,
}

impl Thread {
    async fn process_once(&self) -> bool {
        self.kernel_exec_actor.write().await.process_once().await
    }

    fn do_loop(self) -> ThreadHandle {
        let fut = async move {
            loop {
                self.kernel_queue_evt_rx
                    .consume_events(Duration::from_millis(100))
                    .await;
                while self.process_once().await {}
            }
        };
        let hdl = tokio::spawn(fut).abort_handle();
        ThreadHandle(hdl)
    }
}

pub fn new(
    kernel_exec_actor: Actor<KernelExec>,
    kernel_queue_evt_rx: EventConsumer<KernelQueueEvent>,
) -> ThreadHandle {
    Thread {
        kernel_exec_actor,
        kernel_queue_evt_rx,
    }
    .do_loop()
}
