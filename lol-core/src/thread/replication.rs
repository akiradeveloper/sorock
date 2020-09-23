use super::news;
use crate::{ElectionState, Id, RaftApp, RaftCore};
use std::sync::Arc;
use std::time::Duration;

struct Thread<A> {
    follower_id: Id,
    core: Arc<RaftCore<A>>,
    subscriber: news::Subscriber,
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
                    core.advance_replication(follower_id).await
                }
            })
            .await
            {}
            tokio::time::timeout(Duration::from_millis(100), self.subscriber.wait()).await;
        }
    }
}
pub async fn run<A: RaftApp>(core: Arc<RaftCore<A>>, follower_id: Id) {
    let subscriber = core.log.append_news.lock().await.subscribe();
    let x = Thread {
        core,
        follower_id,
        subscriber,
    };
    x.run().await
}
