use crate::{ElectionState, RaftApp, RaftCore};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::time::delay_for;

struct Thread<A> {
    core: Arc<RaftCore<A>>,
}
impl<A: RaftApp> Thread<A> {
    async fn run(self) {
        loop {
            // randomly timeout to avoid two nodes become candidate at the same time.
            let rand_wait = rand::random::<u64>() % 500;
            let election_timeout: Duration = Duration::from_millis(500 + rand_wait);
            delay_for(election_timeout).await;

            if !self
                .core
                .cluster
                .read()
                .await
                .internal
                .contains_key(&self.core.id)
            {
                continue;
            }

            let core = Arc::clone(&self.core);
            let f = async move {
                let last_time = *core.last_heartbeat_received.lock().await;
                let now = Instant::now();
                let elapsed = now - last_time;
                if elapsed > election_timeout {
                    let election_state = core.vote.read().await.election_state;
                    if std::matches!(election_state, ElectionState::Follower) {
                        log::info!("heartbeat is not received for {} ms", elapsed.as_millis());
                        core.try_promote().await;
                    }
                };
            };
            tokio::spawn(f).await;
        }
    }
}
pub async fn run<A: RaftApp>(core: Arc<RaftCore<A>>) {
    let x = Thread { core };
    x.run().await
}
