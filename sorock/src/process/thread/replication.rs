use super::*;

#[derive(Clone)]
pub struct Thread {
    follower_id: NodeAddress,
    ctrl: Control,
    consumer: EventConsumer<QueueEvent>,
    producer: EventProducer<ReplicationEvent>,
}
impl Thread {
    async fn advance_once(&self) -> Result<bool> {
        let election_state = self.ctrl.read_election_state();
        if !std::matches!(election_state, control::ElectionState::Leader) {
            return Ok(false);
        }

        control::effect::advance_replication::Effect {
            ctrl: self.ctrl.clone(),
        }
        .exec(self.follower_id.clone())
        .await?;

        Ok(true)
    }

    fn do_loop(self) -> ThreadHandle {
        let fut = async move {
            loop {
                self.consumer
                    .consume_events(Duration::from_millis(100))
                    .await;
                while let Ok(true) = self.advance_once().await {
                    self.producer.push_event(ReplicationEvent);
                }
            }
        };
        let hdl = tokio::spawn(fut).abort_handle();
        ThreadHandle(hdl)
    }
}

pub fn new(
    follower_id: NodeAddress,
    ctrl: Control,
    consumer: EventConsumer<QueueEvent>,
    producer: EventProducer<ReplicationEvent>,
) -> ThreadHandle {
    Thread {
        follower_id,
        ctrl,
        consumer,
        producer,
    }
    .do_loop()
}
