use super::*;

#[derive(Clone)]
pub struct Thread {
    command_log: CommandLog,
    peers: Ref<PeerSvc>,
    voter: Ref<Voter>,
}
impl Thread {
    async fn run_once(&self) -> Result<()> {
        let election_state = self.voter.read_election_state();
        ensure!(std::matches!(election_state, voter::ElectionState::Leader));

        let cur_commit_index = self.command_log.commit_pointer.load(Ordering::SeqCst);
        let new_commit_index = self.peers.find_new_commit_index().await?;

        if new_commit_index > cur_commit_index {
            debug!("commit -> {new_commit_index}");
            self.command_log
                .commit_pointer
                .store(new_commit_index, Ordering::SeqCst);
        }

        Ok(())
    }

    fn do_loop(self) -> ThreadHandle {
        let hdl = tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_millis(100));
            loop {
                interval.tick().await;
                let fut = {
                    let this = self.clone();
                    async move { this.run_once().await }
                };
                let _ = defensive_panic_guard(fut).await;
            }
        })
        .abort_handle();

        ThreadHandle(hdl)
    }
}

pub fn new(command_log: CommandLog, peers: Ref<PeerSvc>, voter: Ref<Voter>) -> ThreadHandle {
    Thread {
        command_log,
        peers,
        voter,
    }
    .do_loop()
}
