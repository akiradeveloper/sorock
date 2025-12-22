use super::*;

struct Thread {
    query_exec_actor: Actor<query_queue::QueryExec>,
    app_exec_actor: Actor<AppExec>,
    app_evt_rx: EventConsumer<AppliedEvent>,
}

impl Thread {
    async fn advance_once(&self) -> bool {
        let last_applied = self.app_exec_actor.read().await.applied_index;
        self.query_exec_actor
            .write()
            .await
            .process(last_applied)
            .await
            > 0
    }

    fn do_loop(self) -> ThreadHandle {
        let fut = async move {
            loop {
                self.app_evt_rx
                    .consume_events(Duration::from_millis(100))
                    .await;
                while self.advance_once().await {}
            }
        };
        let hdl = tokio::spawn(fut).abort_handle();
        ThreadHandle(hdl)
    }
}

pub fn new(
    query_queue: Actor<query_queue::QueryExec>,
    app_exec: Actor<AppExec>,
    app_evt_rx: EventConsumer<AppliedEvent>,
) -> ThreadHandle {
    Thread {
        query_exec_actor: query_queue,
        app_exec_actor: app_exec,
        app_evt_rx,
    }
    .do_loop()
}
