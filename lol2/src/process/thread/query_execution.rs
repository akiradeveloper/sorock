use super::*;

#[derive(Clone)]
struct Thread {
    query_queue: QueryQueue,
    command_log: Ref<CommandLog>,
}

impl Thread {
    async fn advance_once(&self) -> bool {
        let last_applied = self.command_log.user_pointer.load(Ordering::SeqCst);
        let cont = self.query_queue.execute(last_applied).await;
        cont
    }

    fn do_loop(self) -> ThreadHandle {
        let hdl = tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_millis(100));
            loop {
                interval.tick().await;
                while self.advance_once().await {}
            }
        })
        .abort_handle();

        ThreadHandle(hdl)
    }
}

pub fn new(query_queue: QueryQueue, command_log: Ref<CommandLog>) -> ThreadHandle {
    Thread {
        query_queue,
        command_log,
    }
    .do_loop()
}
