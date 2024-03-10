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
            loop {
                // Every iteration involves
                // T = 100ms sleep + RPC round trip time.
                // So, heartbeat is observed at follower site every T time.
                // We can't use tokio::time::interval instead because it results in
                // follower receives heartbeat every 100ms regardless of RPC round trip time.
                // In this case, the failure detector at follower site will not work correctly.
                tokio::time::sleep(Duration::from_millis(100)).await;
                self.run_once().await.ok();
            }
        })
        .abort_handle();

        ThreadHandle(hdl)
    }
}

pub fn new(follower_id: NodeId, voter: Ref<Voter>) -> ThreadHandle {
    Thread { follower_id, voter }.do_loop()
}
