use super::*;

pub struct Thread {
    follower_id: ServerAddress,
    replication_actor: Actor<Replication>,
    ctrl_actor: Actor<Control>,
    queue_evt_rx: EventWaiter<QueueEvent>,
    replication_evt_tx: EventNotifier<ReplicationEvent>,
}

impl Thread {
    async fn advance_once(&self) -> Result<bool> {
        if !self.ctrl_actor.read().await.is_leader() {
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

    fn run_loop(self) -> ThreadHandle {
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

pub fn run(
    follower_id: ServerAddress,
    progress: Actor<Replication>,
    ctrl: Actor<Control>,
    queue_evt_rx: EventWaiter<QueueEvent>,
    replication_evt_tx: EventNotifier<ReplicationEvent>,
) -> ThreadHandle {
    Thread {
        follower_id,
        replication_actor: progress,
        ctrl_actor: ctrl,
        queue_evt_rx,
        replication_evt_tx,
    }
    .run_loop()
}
