use crate::{RaftApp, RaftCore};
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;
use sysinfo::SystemExt;

struct Thread<A> {
    core: Arc<RaftCore<A>>,
}
impl<A: RaftApp> Thread<A> {
    async fn run(self) {
        let mut info = sysinfo::System::new();
        loop {
            tokio::time::delay_for(Duration::from_secs(1)).await;
            info.refresh_memory();
            let total_memory_kb = info.get_total_memory() as f64;
            let used_memory_kb = info.get_used_memory() as f64;
            let core = Arc::clone(&self.core);
            let f = async move {
                let usage = used_memory_kb / total_memory_kb;
                if usage > core.tunable.read().await.compaction_memory_limit {
                    log::warn!("start compaction L2");
                    let new_head_index = core.log.last_applied.load(Ordering::SeqCst);
                    core.log
                        .advance_head_log_index(new_head_index, Arc::clone(&core))
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
