use crate::RaftCore;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

struct Thread {
    core: Arc<RaftCore>,
}
impl Thread {
    async fn run(self) {
        loop {
            let interval = self.core.config.read().await.compaction_interval();
            if interval.is_zero() {
                tokio::time::sleep(Duration::from_secs(1)).await;
                continue;
            }

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
pub(crate) async fn run(core: Arc<RaftCore>) {
    let x = Thread { core };
    x.run().await
}
