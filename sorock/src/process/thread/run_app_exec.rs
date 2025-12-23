use super::*;

pub struct Thread {
    pub app_exec_actor: Actor<AppExec>,
    pub app_queue_evt_rx: EventWaiter<AppQueueEvent>,
    pub applied_evt_tx: EventNotifier<AppliedEvent>,
}

impl Thread {
    async fn process_once(&self) -> bool {
        self.app_exec_actor.write().await.process_once().await
    }

    fn run_loop(self) -> ThreadHandle {
        let fut = async move {
            loop {
                self.app_queue_evt_rx
                    .consume_events(Duration::from_millis(100))
                    .await;
                while self.process_once().await {
                    self.applied_evt_tx.push_event(AppliedEvent);
                }
            }
        };
        let hdl = tokio::spawn(fut).abort_handle();
        ThreadHandle(hdl)
    }
}

pub fn run(
    app_exec_actor: Actor<AppExec>,
    app_queue_evt_rx: EventWaiter<AppQueueEvent>,
    applied_evt_tx: EventNotifier<AppliedEvent>,
) -> ThreadHandle {
    Thread {
        app_exec_actor,
        app_queue_evt_rx,
        applied_evt_tx,
    }
    .run_loop()
}
