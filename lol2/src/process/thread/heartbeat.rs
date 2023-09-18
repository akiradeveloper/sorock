use super::*;

#[derive(Clone)]
pub struct Thread {
    follower_id: NodeId,
    voter: Ref<Voter>,
}
impl Thread {
    async fn run_once(&self) -> Result<()> {
        let election_state = self.voter.read_election_state();
        ensure!(std::matches!(election_state, voter::ElectionState::Leader));

        self.voter.send_heartbeat(self.follower_id.clone()).await
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

pub fn new(follower_id: NodeId, voter: Ref<Voter>) -> ThreadHandle {
    Thread { follower_id, voter }.do_loop()
}
