use super::*;

use std::time::Instant;

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
            let mut last = Instant::now();
            let mut interval = tokio::time::interval(Duration::from_millis(300));
            loop {
                interval.tick().await;
                // Periodically sending a new commit state to the buffer.
                self.run_once().await.ok();

                let now = Instant::now();
                if (now - last) > Duration::from_secs(1) {
                    warn!("Heartbeat thread is too slow");
                }
                last = now;
            }
        })
        .abort_handle();

        ThreadHandle(hdl)
    }
}

pub fn new(follower_id: NodeId, voter: Ref<Voter>) -> ThreadHandle {
    Thread { follower_id, voter }.do_loop()
}
