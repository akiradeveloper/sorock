use crate::{ElectionState, RaftApp, RaftCore, ELECTION_TIMEOUT_MS};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::time::delay_for;

struct Thread<A: RaftApp> {
    core: Arc<RaftCore<A>>,
}
impl<A: RaftApp> Thread<A> {
    async fn run(self) {
        loop {
            // randomly timeout to avoid two nodes become candidate at the same time.
            let rand_part = rand::random::<u64>() % ELECTION_TIMEOUT_MS;
            let rand_timeout = Duration::from_millis(ELECTION_TIMEOUT_MS + rand_part);
            delay_for(rand_timeout).await;

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
                if elapsed > Duration::from_millis(ELECTION_TIMEOUT_MS) {
                    let election_state = *core.election_state.read().await;
                    if std::matches!(election_state, ElectionState::Follower) {
                        log::info!("heartbeat is not received for {} ms", elapsed.as_millis());
                        core.try_promote(false).await.unwrap();
                    }
                };
            };
            let _ = tokio::spawn(f).await;
        }
    }
}
pub async fn run<A: RaftApp>(core: Arc<RaftCore<A>>) {
    let x = Thread { core };
    x.run().await
}
