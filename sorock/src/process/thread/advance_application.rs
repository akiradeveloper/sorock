use super::*;

#[derive(Clone)]
pub struct Thread {
    command_log: Actor<CommandLog>,
    consumer: EventConsumer<KernEvent>,
    producer: EventProducer<ApplicationEvent>,
}

impl Thread {
    async fn advance_once(&self) -> Result<()> {
        command_log::effect::advance_application::Effect {
            command_log: &mut *self.command_log.write().await,
        }
        .exec()
        .await
    }

    fn do_loop(self) -> ThreadHandle {
        let fut = async move {
            loop {
                self.consumer
                    .consume_events(Duration::from_millis(100))
                    .await;
                while self.advance_once().await.is_ok() {
                    self.producer.push_event(ApplicationEvent);
                }
            }
        };
        let hdl = tokio::spawn(fut).abort_handle();
        ThreadHandle(hdl)
    }
}

pub fn new(
    command_log: Actor<CommandLog>,
    consumer: EventConsumer<KernEvent>,
    producer: EventProducer<ApplicationEvent>,
) -> ThreadHandle {
    Thread {
        command_log,
        consumer,
        producer,
    }
    .do_loop()
}
