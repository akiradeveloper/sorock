use super::*;

#[derive(Clone)]
struct Thread {
    query_queue: query_queue::Processor,
    command_log: Ref<CommandLog>,
    consumer: EventConsumer<ApplicationEvent>,
}

impl Thread {
    async fn advance_once(&self) -> bool {
        let last_applied = self.command_log.user_pointer.load(Ordering::SeqCst);
        self.query_queue.process(last_applied) > 0
    }

    fn do_loop(self) -> ThreadHandle {
        let fut = async move {
            loop {
                self.consumer
                    .consume_events(Duration::from_millis(100))
                    .await;
                while self.advance_once().await {
                    tokio::task::yield_now().await;
                }
            }
        };
        let hdl = tokio::spawn(fut).abort_handle();
        ThreadHandle(hdl)
    }
}

pub fn new(
    query_queue: query_queue::Processor,
    command_log: Ref<CommandLog>,
    consumer: EventConsumer<ApplicationEvent>,
) -> ThreadHandle {
    Thread {
        query_queue,
        command_log,
        consumer,
    }
    .do_loop()
}
