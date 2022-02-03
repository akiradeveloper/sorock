use crate::{ElectionState, RaftCore};
use std::sync::Arc;
use std::time::Duration;

struct Thread {
    core: Arc<RaftCore>,
}
impl Thread {
    async fn run(self) {
        loop {
            tokio::time::sleep(Duration::from_millis(100)).await;

            if !self
                .core
                .cluster
                .read()
                .await
                .membership
                .contains(&self.core.id)
            {
                continue;
            }

            if !std::matches!(
                *self.core.election_state.read().await,
                ElectionState::Follower
            ) {
                continue;
            }
            if !self.core.detect_election_timeout().await {
                continue;
            }

            let normal_dist = &self
                .core
                .failure_detector
                .read()
                .await
                .detector
                .normal_dist();
            let base_timeout = (normal_dist.mu() + normal_dist.sigma() * 4).as_millis();
            let rand_timeout = rand::random::<u128>() % base_timeout;
            tokio::time::sleep(Duration::from_millis(rand_timeout as u64)).await;
            // Double-check
            if !self.core.detect_election_timeout().await {
                continue;
            }

            let core = Arc::clone(&self.core);
            let f = async move {
                log::info!("heartbeat is not received for a long time");
                core.try_promote(false).await.unwrap();
            };
            let _ = tokio::spawn(f).await;
        }
    }
}
pub(crate) async fn run(core: Arc<RaftCore>) {
    let x = Thread { core };
    x.run().await
}
