use super::*;

use futures::StreamExt;
use std::{pin::Pin, time::Duration};
use tonic::transport::Uri;

pub fn connect_real_node(uri: Uri, shard_index: u32) -> impl model::stream::Node {
    let chan = Endpoint::from(uri).connect_lazy();
    let client = proto::raft_client::RaftClient::new(chan);
    RealNode {
        client,
        shard_index,
    }
}

struct RealNode {
    client: proto::raft_client::RaftClient<Channel>,
    shard_index: u32,
}

#[async_trait::async_trait]
impl model::stream::Node for RealNode {
    async fn watch_membership(&self) -> Pin<Box<dyn Stream<Item = proto::Membership> + Send>> {
        let shard = proto::Shard {
            id: self.shard_index,
        };
        let mut client = self.client.clone();
        let st = async_stream::stream! {
            loop {
                tokio::time::sleep(Duration::from_secs(5)).await;
                match client.get_membership(shard).await {
                    Ok(response) => {
                        let membership = response.into_inner();
                        yield membership
                    }
                    Err(e) => {
                        eprintln!("Failed to get membership: {}", e);
                        continue;
                    }
                }
            }
        };
        Box::pin(st)
    }

    async fn watch_log_metrics(
        &self,
        _: Uri,
    ) -> Pin<Box<dyn Stream<Item = proto::LogMetrics> + Send>> {
        let shard = proto::Shard {
            id: self.shard_index,
        };
        let mut client = self.client.clone();
        let st = async_stream::stream! {
            match client.watch_log_metrics(shard).await {
                Ok(response) => {
                    let mut st = response.into_inner();
                    while let Some(result) = st.next().await {
                        match result {
                            Ok(metrics) => yield metrics,
                            Err(e) => {
                                eprintln!("Error in log metrics stream: {}", e);
                                break;
                            }
                        }
                    }
                }
                Err(e) => {
                    eprintln!("Failed to watch log metrics: {}", e);
                }
            }
        };
        Box::pin(st)
    }
}
