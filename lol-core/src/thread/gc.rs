use crate::{RaftApp, RaftCore};
use std::sync::Arc;
use std::time::Duration;

struct Thread {
    core: Arc<RaftCore>,
}
impl Thread {
    async fn run(self) {
        loop {
            tokio::time::sleep(Duration::from_secs(1)).await;
            let core = Arc::clone(&self.core);
            let f = async move {
                core.log.run_gc(Arc::clone(&core)).await.unwrap();
            };
            let _ = tokio::spawn(f).await;
        }
    }
}
pub async fn run(core: Arc<RaftCore>) {
    let x = Thread { core };
    x.run().await
}
