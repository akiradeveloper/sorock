use crate::{ElectionState, RaftApp, RaftCore};
use std::sync::Arc;
use std::time::Duration;

struct Thread<A: RaftApp> {
    core: Arc<RaftCore<A>>,
}
impl<A: RaftApp> Thread<A> {
    async fn run(self) {
        loop {
            tokio::time::delay_for(Duration::from_millis(100)).await;

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
                let election_state = *core.election_state.read().await;
                if std::matches!(election_state, ElectionState::Leader) {
                    core.broadcast_heartbeat().await.unwrap();
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
