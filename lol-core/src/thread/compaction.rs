use crate::{RaftApp, RaftCore};
use std::sync::Arc;
use std::time::Duration;
use std::sync::atomic::Ordering;

struct Thread<A: RaftApp> {
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

            let core = Arc::clone(&self.core);
            let f = async move {
                log::info!("create fold snapshot");
                let new_snapshot_index = core.log.last_applied.load(Ordering::SeqCst);
                log::info!("new snapshot index: {:?}", new_snapshot_index);
                core.log
                    .create_fold_snapshot(new_snapshot_index, Arc::clone(&core))
                    .await;
            };
            tokio::spawn(f).await;
        }
    }
}
pub async fn run<A: RaftApp>(core: Arc<RaftCore<A>>) {
    let x = Thread { core };
    x.run().await
}
