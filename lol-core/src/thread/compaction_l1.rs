use crate::{RaftApp, RaftCore};
use std::sync::Arc;
use std::time::Duration;

struct Thread<A> {
    core: Arc<RaftCore<A>>,
}
impl<A: RaftApp> Thread<A> {
    async fn run(self) {
        loop {
            let v = self.core.tunable.read().await.compaction_interval_sec;
            if v == 0 {
                tokio::time::delay_for(Duration::from_secs(1)).await;
                continue;
            }

            let interval = Duration::from_secs(v);
            tokio::time::delay_for(interval).await;

            let delay = Duration::from_secs(self.core.tunable.read().await.compaction_delay_sec);
            let core = Arc::clone(&self.core);
            let f = async move {
                log::info!("start compaction L1");
                let new_snapshot_index = core.log.find_compaction_point(delay).await;
                log::info!("new compaction point: {:?}", new_snapshot_index);
                if let Some(new_snapshot_index) = new_snapshot_index {
                    core.log
                        .advance_snapshot_index(new_snapshot_index, Arc::clone(&core))
                        .await;
                }
            };
            tokio::spawn(f).await;
        }
    }
}
pub async fn run<A: RaftApp>(core: Arc<RaftCore<A>>) {
    let x = Thread { core };
    x.run().await
}
