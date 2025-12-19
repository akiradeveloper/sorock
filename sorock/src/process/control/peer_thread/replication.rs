use super::*;

#[derive(Clone)]
pub struct Thread {
    follower_id: NodeAddress,
    progress: ReplicationProgressActor,
    ctrl_actor: Read<ControlActor>,
    consumer: thread::EventConsumer<thread::QueueEvent>,
    producer: thread::EventProducer<thread::ReplicationEvent>,
}
impl Thread {
    async fn advance_once(&self) -> Result<bool> {
        let election_state = self.ctrl_actor.read().await.read_election_state();
        if !std::matches!(election_state, control::ElectionState::Leader) {
            return Ok(false);
        }

        control::effect::advance_replication::Effect {
            progress: &mut *self.progress.lock().await,
            ctrl: &*self.ctrl_actor.read().await,
        }
        .exec(self.follower_id.clone())
        .await?;

        Ok(true)
    }

    fn do_loop(self) -> thread::ThreadHandle {
        let fut = async move {
            loop {
                self.consumer
                    .consume_events(Duration::from_millis(100))
                    .await;
                while let Ok(true) = self.advance_once().await {
                    self.producer.push_event(thread::ReplicationEvent);
                }
            }
        };
        let hdl = tokio::spawn(fut).abort_handle();
        thread::ThreadHandle(hdl)
    }
}

pub fn new(
    follower_id: NodeAddress,
    progress: Arc<Mutex<ReplicationProgress>>,
    ctrl: Read<ControlActor>,
    consumer: thread::EventConsumer<thread::QueueEvent>,
    producer: thread::EventProducer<thread::ReplicationEvent>,
) -> thread::ThreadHandle {
    Thread {
        follower_id,
        progress,
        ctrl_actor: ctrl,
        consumer,
        producer,
    }
    .do_loop()
}
