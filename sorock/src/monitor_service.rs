use super::*;

use std::pin::Pin;

mod proto {
    pub use super::generated::sorock_monitor::*;
}
use proto::*;

pub fn new(node: RaftNode) -> monitor_server::MonitorServer<impl monitor_server::Monitor> {
    proto::monitor_server::MonitorServer::new(App { node })
}

struct App {
    node: RaftNode,
}

#[tonic::async_trait]
impl monitor_server::Monitor for App {
    async fn get_membership(
        &self,
        req: tonic::Request<Shard>,
    ) -> std::result::Result<tonic::Response<Membership>, tonic::Status> {
        let shard_id = req.into_inner().id;
        let process = self
            .node
            .get_process(shard_id)
            .context(Error::ProcessNotFound(shard_id))
            .unwrap();
        let members = process.get_membership().await.unwrap().members;
        let out = Membership {
            members: members.into_iter().map(|x| x.to_string()).collect(),
        };
        Ok(tonic::Response::new(out))
    }

    type GetLogMetricsStream =
        Pin<Box<dyn Stream<Item = Result<LogMetrics, tonic::Status>> + Send>>;

    async fn get_log_metrics(
        &self,
        req: tonic::Request<Shard>,
    ) -> std::result::Result<tonic::Response<Self::GetLogMetricsStream>, tonic::Status> {
        let shard_id = req.into_inner().id;
        let node = self.node.clone();
        let st = async_stream::try_stream! {
            let mut intvl = tokio::time::interval(std::time::Duration::from_secs(1));
            loop {
                intvl.tick().await;

                let process = node
                    .get_process(shard_id)
                    .context(Error::ProcessNotFound(shard_id))
                    .unwrap();
                let log_state = process.get_log_state().await.unwrap();
                let metrics = LogMetrics {
                    head_index: log_state.head_index,
                    snap_index: log_state.snap_index,
                    app_index: log_state.app_index,
                    commit_index: log_state.commit_index,
                    last_index: log_state.last_index,
                };
                yield metrics
            }
        };
        Ok(tonic::Response::new(Box::pin(st)))
    }
}
