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
            while let Ok(true) = tokio::spawn({
                let core = Arc::clone(&self.core);
                async move {
                    let need_work = core.log.last_applied.load(Ordering::SeqCst)
                        < core.log.commit_index.load(Ordering::SeqCst);
                    if need_work {
                        core.log
                            .advance_last_applied(Arc::clone(&core))
                            .await
                            .unwrap();
                    }
                    need_work
                }
            })
            .await
            {}
            let _ = tokio::time::timeout(
                Duration::from_millis(100),
                self.core.log.commit_notify.notified(),
            )
            .await;
        }
    }
}
pub async fn run(core: Arc<RaftCore>) {
    let x = Thread { core };
    x.run().await
}
