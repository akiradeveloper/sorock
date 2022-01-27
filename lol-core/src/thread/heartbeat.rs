use crate::{ElectionState, Id, RaftApp, RaftCore};
use std::sync::Arc;
use std::time::Duration;

struct Thread {
    follower_id: Id,
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

            let core = Arc::clone(&self.core);
            let follower_id = self.follower_id.clone();
            let f = async move {
                let election_state = *core.election_state.read().await;
                if std::matches!(election_state, ElectionState::Leader) {
                    core.send_heartbeat(follower_id).await.unwrap();
                };
            };
            let _ = tokio::spawn(f).await;
        }
    }
}
pub(crate) async fn run(core: Arc<RaftCore>, follower_id: Id) {
    let x = Thread { core, follower_id };
    x.run().await
}
