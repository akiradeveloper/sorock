use super::*;

#[derive(Clone)]
struct Thread {
    ready_queue: query_queue::ReadyQueue,
    command_log: Read<CommandLogActor>,
    consumer: EventConsumer<ApplicationEvent>,
}

impl Thread {
    async fn advance_once(&self) -> bool {
        let last_applied = self.command_log.read().await.application_pointer;
        self.ready_queue.process(last_applied).await > 0
    }

    fn do_loop(self) -> ThreadHandle {
        let fut = async move {
            loop {
                self.consumer
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
    query_queue: query_queue::ReadyQueue,
    command_log: Read<CommandLogActor>,
    consumer: EventConsumer<ApplicationEvent>,
) -> ThreadHandle {
    Thread {
        ready_queue: query_queue,
        command_log,
        consumer,
    }
    .do_loop()
}
