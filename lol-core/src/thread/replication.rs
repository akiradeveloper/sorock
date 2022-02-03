use crate::{ElectionState, Id, RaftCore};
use std::sync::Arc;
use std::time::Duration;

struct Thread {
    follower_id: Id,
    core: Arc<RaftCore>,
}
impl Thread {
    async fn run(self) {
        log::info!("replicator launched for {}", self.follower_id.uri());
        loop {
            while let Ok(true) = tokio::spawn({
                let core = Arc::clone(&self.core);
                let follower_id = self.follower_id.clone();
                async move {
                    let election_state = *core.election_state.read().await;
                    if !std::matches!(election_state, ElectionState::Leader) {
                        return false;
                    }
                    core.advance_replication(follower_id).await.unwrap()
                }
            })
            .await
            {}
            let _ = tokio::time::timeout(
                Duration::from_millis(100),
                self.core.log.append_notify.notified(),
            )
            .await;
        }
    }
}
pub(crate) async fn run(core: Arc<RaftCore>, follower_id: Id) {
    let x = Thread { core, follower_id };
    x.run().await
}
