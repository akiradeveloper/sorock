use super::*;

#[derive(Clone)]
pub struct Thread {
    voter: Voter,
    command_log: Ref<CommandLog>,
    peers: Ref<PeerSvc>,
}

impl Thread {
    pub async fn run_once(&self) -> Result<()> {
        voter::task::try_stepdown::Task {
            voter: self.voter.clone(),
            command_log: self.command_log.clone(),
            peers: self.peers.clone(),
        }.exec().await?;

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

pub fn new(voter: Voter, command_log: Ref<CommandLog>, peers: Ref<PeerSvc>) -> ThreadHandle {
    Thread { voter, command_log, peers }.do_loop()
}
