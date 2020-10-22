use super::notification;
use crate::{RaftApp, RaftCore};
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

struct Thread<A: RaftApp> {
    core: Arc<RaftCore<A>>,
    subscriber: notification::Subscriber,
}
impl<A: RaftApp> Thread<A> {
    async fn run(self) {
        loop {
            while let Ok(true) = tokio::spawn({
                let core = Arc::clone(&self.core);
                async move {
                    let cur_last_applied = core.log.last_applied.load(Ordering::SeqCst);
                    core.query_queue
                        .lock()
                        .await
                        .execute(cur_last_applied, Arc::clone(&core))
                        .await
                }
            })
            .await
            {}
            let _ = tokio::time::timeout(Duration::from_millis(100), self.subscriber.wait()).await;
        }
    }
}
pub async fn run<A: RaftApp>(core: Arc<RaftCore<A>>) {
    let subscriber = core.log.apply_notification.lock().await.subscribe();
    let x = Thread { core, subscriber };
    x.run().await
}
