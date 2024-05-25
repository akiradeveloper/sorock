use super::*;

#[derive(Clone)]
pub struct Thread {
    follower_id: NodeId,
    peers: PeerSvc,
    voter: Ref<Voter>,
}
impl Thread {
    async fn advance_once(&self) -> Result<bool> {
        let election_state = self.voter.read_election_state();
        if !std::matches!(election_state, voter::ElectionState::Leader) {
            return Ok(false);
        }

        self.peers
            .advance_replication(self.follower_id.clone())
            .await?;

        Ok(true)
    }

    fn do_loop(self) -> ThreadHandle {
        let fut = async move {
            let mut interval = tokio::time::interval(Duration::from_millis(100));
            loop {
                interval.tick().await;
                while let Ok(true) = self.advance_once().await {
                    tokio::task::yield_now().await;
                }
            }
        };
        let fut = tokio::task::unconstrained(fut);
        let hdl = tokio::spawn(fut).abort_handle();
        ThreadHandle(hdl)
    }
}

pub fn new(follower_id: NodeId, peers: PeerSvc, voter: Ref<Voter>) -> ThreadHandle {
    Thread {
        follower_id,
        peers,
        voter,
    }
    .do_loop()
}
