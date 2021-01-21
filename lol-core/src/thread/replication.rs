use crate::{ElectionState, Id, RaftApp, RaftCore};
use std::sync::Arc;
use std::time::Duration;

struct Thread<A: RaftApp> {
    follower_id: Id,
    core: Arc<RaftCore<A>>,
}
impl<A: RaftApp> Thread<A> {
    async fn run(self) {
        log::info!("replicator launched for {}", self.follower_id);
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
            let _ = tokio::time::timeout(Duration::from_millis(100), self.core.log.append_notify.notified()).await;
        }
    }
}
pub async fn run<A: RaftApp>(core: Arc<RaftCore<A>>, follower_id: Id) {
    let x = Thread {
        core,
        follower_id,
    };
    x.run().await
}
