use super::*;

#[derive(Clone)]
pub struct Thread {
    command_log: CommandLog,
    voter: Voter,
    consumer: EventConsumer<CommitEvent>,
    producer: EventProducer<KernEvent>,
}
impl Thread {
    async fn advance_once(&self) -> Result<()> {
        command_log::effect::advance_kern_process::Effect {
            command_log: self.command_log.clone(),
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
    command_log: CommandLog,
    voter: Voter,
    consumer: EventConsumer<CommitEvent>,
    producer: EventProducer<KernEvent>,
) -> ThreadHandle {
    Thread {
        command_log,
        voter,
        consumer,
        producer,
    }
    .do_loop()
}
