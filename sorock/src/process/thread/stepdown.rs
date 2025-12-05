use super::*;

#[derive(Clone)]
pub struct Thread {
    voter: Voter,
    state_mechine: Read<StateMachine>,
    peers: Read<Peers>,
}

impl Thread {
    pub async fn run_once(&self) -> Result<()> {
        voter::effect::try_stepdown::Effect {
            voter: self.voter.clone(),
            // state_mechine: self.state_mechine.clone(),
            // peers: self.peers.clone(),
        }
        .exec()
        .await?;

        Ok(())
    }

    pub fn do_loop(self) -> ThreadHandle {
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

pub fn new(voter: Voter, state_mechine: Read<StateMachine>, peers: Read<Peers>) -> ThreadHandle {
    Thread {
        voter,
        state_mechine,
        peers,
    }
    .do_loop()
}
