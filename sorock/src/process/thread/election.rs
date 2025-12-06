use super::*;

#[derive(Clone)]
pub struct Thread {
    voter: Voter,
    state_mechine: StateMachine,
    peers: Peers,
}
impl Thread {
    async fn run_once(&self) -> Result<()> {
        let election_state = self.voter.read_election_state();
        ensure!(std::matches!(
            election_state,
            voter::ElectionState::Follower
        ));

        // sleep random duration
        if let Some(timeout) = self.voter.get_election_timeout() {
            tokio::time::sleep(timeout).await;
        }
        // if it doesn't receive any heartbeat from a leader (or new leader)
        // it try to become a leader.
        if self.voter.get_election_timeout().is_some() {
            info!("election timeout. try to become a leader");
            voter::effect::try_promote::Effect {
                voter: self.voter.clone(),
                state_mechine: self.state_mechine.clone(),
                peers: self.peers.clone(),
            }
            .exec(false)
            .await?;
        }
        Ok(())
    }

    fn do_loop(self) -> ThreadHandle {
        let fut = async move {
            let mut interval = tokio::time::interval(Duration::from_secs(1));
            loop {
                interval.tick().await;
                self.run_once().await.ok();
            }
        };
        let hdl = tokio::spawn(fut).abort_handle();
        ThreadHandle(hdl)
    }
}

pub fn new(voter: Voter, state_mechine: StateMachine, peers: Peers) -> ThreadHandle {
    Thread {
        voter,
        state_mechine,
        peers,
    }
    .do_loop()
}
