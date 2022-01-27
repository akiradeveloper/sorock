use crate::{RaftApp, RaftCore};
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

struct Thread {
    core: Arc<RaftCore>,
}
impl Thread {
    async fn run(self) {
        loop {
            let v = self.core.tunable.read().await.compaction_interval_sec;
            if v == 0 {
                tokio::time::sleep(Duration::from_secs(1)).await;
                continue;
            }

            let interval = Duration::from_secs(v);
            tokio::time::sleep(interval).await;

            let core = Arc::clone(&self.core);
            let f = async move {
                let new_snapshot_index = core.log.last_applied.load(Ordering::SeqCst);
                core.log
                    .create_fold_snapshot(new_snapshot_index, Arc::clone(&core))
                    .await
                    .unwrap();
            };
            let _ = tokio::spawn(f).await;
        }
    }
}
pub async fn run(core: Arc<RaftCore>) {
    let x = Thread { core };
    x.run().await
}
