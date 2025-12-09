use super::*;

#[derive(Clone)]
pub struct Thread {
    state_machine: StateMachine,
    voter: Voter,
    consumer: EventConsumer<CommitEvent>,
    producer: EventProducer<KernEvent>,
}
impl Thread {
    async fn advance_once(&self) -> Result<()> {
        state_machine::effect::advance_kernel::Effect {
            state_machine: self.state_machine.clone(),
            voter: self.voter.clone(),
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
                    self.producer.push_event(KernEvent);
                }
            }
        };
        let hdl = tokio::spawn(fut).abort_handle();
        ThreadHandle(hdl)
    }
}

pub fn new(
    state_machine: StateMachine,
    voter: Voter,
    consumer: EventConsumer<CommitEvent>,
    producer: EventProducer<KernEvent>,
) -> ThreadHandle {
    Thread {
        state_machine,
        voter,
        consumer,
        producer,
    }
    .do_loop()
}
