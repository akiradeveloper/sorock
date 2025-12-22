use super::*;

#[derive(Clone)]
pub struct Thread {
    ctrl: Actor<Control>,
    consumer: EventConsumer<ReplicationEvent>,
    producer: EventProducer<CommitEvent>,
}
impl Thread {
    async fn run_once(&self) -> Result<()> {
        let election_state = self.ctrl.read().await.read_election_state();
        ensure!(std::matches!(
            election_state,
            control::ElectionState::Leader
        ));

        let cur_commit_index = self.ctrl.read().await.commit_pointer;
        let new_commit_index = self.ctrl.read().await.find_new_commit_index().await?;

        if new_commit_index > cur_commit_index {
            self.ctrl
                .write()
                .await
                .advance_commit_index(new_commit_index);
            self.producer.push_event(CommitEvent);
        }

        Ok(())
    }

    fn do_loop(self) -> ThreadHandle {
        let fut = async move {
            loop {
                self.consumer
                    .consume_events(Duration::from_millis(100))
                    .await;
                self.run_once().await.ok();
            }
        };
        let hdl = tokio::spawn(fut).abort_handle();
        ThreadHandle(hdl)
    }
}

pub fn new(
    ctrl: Actor<Control>,
    consume: EventConsumer<ReplicationEvent>,
    produce: EventProducer<CommitEvent>,
) -> ThreadHandle {
    Thread {
        ctrl,
        consumer: consume,
        producer: produce,
    }
    .do_loop()
}
