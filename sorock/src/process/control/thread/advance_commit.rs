use super::*;

pub struct Thread {
    ctrl_actor: Actor<Control>,
    replication_evt_rx: EventConsumer<ReplicationEvent>,
    commit_evt_tx: EventProducer<CommitEvent>,
}

impl Thread {
    async fn run_once(&self) -> Result<()> {
        let election_state = self.ctrl_actor.read().await.read_election_state();
        ensure!(std::matches!(
            election_state,
            control::ElectionState::Leader
        ));

        let cur_commit_index = self.ctrl_actor.read().await.commit_pointer;
        let new_commit_index = self.ctrl_actor.read().await.find_new_commit_index().await?;

        if new_commit_index > cur_commit_index {
            self.ctrl_actor
                .write()
                .await
                .advance_commit_index(new_commit_index);
            self.commit_evt_tx.push_event(CommitEvent);
        }

        Ok(())
    }

    fn do_loop(self) -> ThreadHandle {
        let fut = async move {
            loop {
                self.replication_evt_rx
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
        ctrl_actor: ctrl,
        replication_evt_rx: consume,
        commit_evt_tx: produce,
    }
    .do_loop()
}
