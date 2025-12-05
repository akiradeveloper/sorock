use super::*;

#[derive(Clone)]
pub struct Thread {
    command_log: CommandLog,
    app: App,
    consumer: EventConsumer<KernEvent>,
    producer: EventProducer<ApplicationEvent>,
}

impl Thread {
    async fn advance_once(&self) -> Result<()> {
        command_log::effect::advance_user_process::Effect {
            command_log: self.command_log.clone(),
            app: self.app.clone(),
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
    command_log: CommandLog,
    app: App,
    consumer: EventConsumer<KernEvent>,
    producer: EventProducer<ApplicationEvent>,
) -> ThreadHandle {
    Thread {
        command_log,
        app,
        consumer,
        producer,
    }
    .do_loop()
}
