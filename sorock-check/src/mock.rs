use super::*;

use futures::stream::Stream;
use std::time::Instant;
use tonic::transport::Uri;

pub struct MockNode {
    start_time: Instant,
}
impl MockNode {
    pub fn new() -> Self {
        Self {
            start_time: Instant::now(),
        }
    }
}

#[async_trait::async_trait]
impl model::stream::Node for MockNode {
    async fn watch_membership(&self) -> Pin<Box<dyn Stream<Item = proto::Membership> + Send>> {
        let out = proto::Membership {
            members: vec![
                "http://n1:4000".to_string(),
                "http://n2:4000".to_string(),
                "http://n3:4000".to_string(),
                "http://n4:4000".to_string(),
                "http://n5:4000".to_string(),
                "http://n6:4000".to_string(),
                "http://n7:4000".to_string(),
                "http://n8:4000".to_string(),
            ],
        };
        Box::pin(futures::stream::once(async move { out }))
    }

    async fn watch_log_metrics(
        &self,
        _: Uri,
    ) -> Pin<Box<dyn Stream<Item = proto::LogMetrics> + Send>> {
        let start_time = self.start_time;
        let st = async_stream::stream! {
            loop {
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                let x = Instant::now().duration_since(start_time).as_secs();
                // f(x) = x^2 * log(x)
                // is a monotonically increasing function
                let f = |x: u64| {
                    let a = f64::powf(x as f64, 2.);
                    let b = f64::log(10.0, x as f64);
                    (a * b) as u64
                };
                let metrics = proto::LogMetrics {
                    head_index: f(x),
                    snapshot_index: f(x+1),
                    application_index: f(x+2),
                    commit_index: f(x+3),
                    last_index: f(x+4),
                };
                yield metrics
            }
        };
        Box::pin(st)
    }
}

pub fn connect_mock_node() -> impl model::stream::Node {
    let app = MockNode::new();
    app
}
