use super::*;

#[derive(Clone)]
pub struct Thread {
    state_mechine: StateMachine,
    peers: Read<Peers>,
    voter: Read<Voter>,
    consumer: EventConsumer<ReplicationEvent>,
    producer: EventProducer<CommitEvent>,
}
impl Thread {
    async fn run_once(&self) -> Result<()> {
        let election_state = self.voter.read_election_state();
        ensure!(std::matches!(election_state, voter::ElectionState::Leader));

        let cur_commit_index = self.state_mechine.commit_pointer.load(Ordering::SeqCst);
        let new_commit_index = self.peers.find_new_commit_index().await?;

        if new_commit_index > cur_commit_index {
            self.state_mechine
                .commit_pointer
                .fetch_max(new_commit_index, Ordering::SeqCst);
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
    state_mechine: StateMachine,
    peers: Read<Peers>,
    voter: Read<Voter>,
    consume: EventConsumer<ReplicationEvent>,
    produce: EventProducer<CommitEvent>,
) -> ThreadHandle {
    Thread {
        state_mechine,
        peers,
        voter,
        consumer: consume,
        producer: produce,
    }
    .do_loop()
}
