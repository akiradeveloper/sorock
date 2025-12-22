use super::*;

pub struct Thread {
    follower_id: NodeAddress,
    replication_actor: Actor<Replication>,
    ctrl_actor: Read<Actor<Control>>,
    queue_evt_rx: EventConsumer<QueueEvent>,
    replication_evt_tx: EventProducer<ReplicationEvent>,
}

impl Thread {
    async fn advance_once(&self) -> Result<bool> {
        let election_state = self.ctrl_actor.read().await.read_election_state();
        if !std::matches!(election_state, control::ElectionState::Leader) {
            return Ok(false);
        }

        control::effect::advance_replication::Effect {
            progress: &mut *self.replication_actor.write().await,
            ctrl: &*self.ctrl_actor.read().await,
        }
        .exec(self.follower_id.clone())
        .await?;

        Ok(true)
    }

    fn do_loop(self) -> ThreadHandle {
        let fut = async move {
            loop {
                self.queue_evt_rx
                    .consume_events(Duration::from_millis(100))
                    .await;
                while let Ok(true) = self.advance_once().await {
                    self.replication_evt_tx.push_event(ReplicationEvent);
                }
            }
        };
        let hdl = tokio::spawn(fut).abort_handle();
        ThreadHandle(hdl)
    }
}

pub fn new(
    follower_id: NodeAddress,
    progress: Actor<Replication>,
    ctrl: Read<Actor<Control>>,
    queue_evt_rx: EventConsumer<QueueEvent>,
    replication_evt_tx: EventProducer<ReplicationEvent>,
) -> ThreadHandle {
    Thread {
        follower_id,
        replication_actor: progress,
        ctrl_actor: ctrl,
        queue_evt_rx,
        replication_evt_tx,
    }
    .do_loop()
}
