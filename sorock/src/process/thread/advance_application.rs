use super::*;

#[derive(Clone)]
pub struct Thread {
    state_machine: StateMachine,
    consumer: EventConsumer<KernEvent>,
    producer: EventProducer<ApplicationEvent>,
}

impl Thread {
    async fn advance_once(&self) -> Result<()> {
        state_machine::effect::advance_application::Effect {
            state_machine: self.state_machine.clone(),
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
    state_machine: StateMachine,
    consumer: EventConsumer<KernEvent>,
    producer: EventProducer<ApplicationEvent>,
) -> ThreadHandle {
    Thread {
        state_machine,
        consumer,
        producer,
    }
    .do_loop()
}
