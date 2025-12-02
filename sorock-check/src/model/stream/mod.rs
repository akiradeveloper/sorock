use super::*;

pub mod log_metrics;
mod membership;

pub use log_metrics::CopyLogMetrics;
pub use membership::CopyMembership;

#[async_trait::async_trait]
pub trait Node: Send + Sync {
    async fn watch_membership(&self) -> Pin<Box<dyn Stream<Item = proto::Membership> + Send>>;
    async fn watch_log_metrics(
        &self,
        url: Uri,
    ) -> Pin<Box<dyn Stream<Item = proto::LogMetrics> + Send>>;
}
