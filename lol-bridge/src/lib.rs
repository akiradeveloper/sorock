use async_trait::async_trait;
use lol_core::{Message, RaftApp, Index};
use std::convert::TryFrom;
use tonic::transport::{Channel, Endpoint, Uri};

use std::error::Error;
#[cfg(unix)]
use tokio::net::UnixStream;
use tower::service_fn;

mod protoimpl {
    tonic::include_proto!("lol_bridge");
}
use protoimpl::app_bridge_client::AppBridgeClient;

#[derive(Clone)]
pub enum BridgeConfig {
    // ip:port
    IPSocket(String),
    // file path
    UnixDomainSocket(String),
}
pub struct RaftAppBridge {
    config: BridgeConfig,
}
#[async_trait]
impl RaftApp for RaftAppBridge {
    type Snapshot = lol_core::snapshot::BytesSnapshot;
    async fn process_message(&self, request: Message) -> anyhow::Result<Message> {
        let chan = Self::connect(self.config.clone()).await?;
        let mut cli = AppBridgeClient::new(chan);
        let req = protoimpl::ProcessMessageReq { message: request };
        let protoimpl::ProcessMessageRep { message } = cli.process_message(req).await?.into_inner();
        Ok(message)
    }
    async fn apply_message(&self, request: Message, apply_index: Index) -> anyhow::Result<(Message, Option<Self::Snapshot>)> {
        let chan = Self::connect(self.config.clone()).await?;
        let mut cli = AppBridgeClient::new(chan);
        let req = protoimpl::ApplyMessageReq { message: request, apply_index, };
        let protoimpl::ApplyMessageRep { message, snapshot } = cli.apply_message(req).await?.into_inner();
        let snapshot = snapshot.map(|x| x.into());
        Ok((message, snapshot))
    }
    async fn install_snapshot(&self, snapshot: Option<&Self::Snapshot>, apply_index: Index) -> anyhow::Result<()> {
        let chan = Self::connect(self.config.clone()).await?;
        let mut cli = AppBridgeClient::new(chan);
        let req = protoimpl::InstallSnapshotReq {
            snapshot: snapshot.map(|x| x.as_ref().to_vec()),
            apply_index,
        };
        cli.install_snapshot(req).await?;
        Ok(())
    }
    async fn fold_snapshot(
        &self,
        old_snapshot: Option<&Self::Snapshot>,
        requests: Vec<Message>,
    ) -> anyhow::Result<Self::Snapshot> {
        let chan = Self::connect(self.config.clone()).await?;
        let mut cli = AppBridgeClient::new(chan);
        let req = protoimpl::FoldSnapshotReq {
            snapshot: old_snapshot.map(|x| x.as_ref().to_vec()),
            messages: requests,
        };
        let protoimpl::FoldSnapshotRep { snapshot } = cli.fold_snapshot(req).await?.into_inner();
        Ok(snapshot.into())
    }
}
impl RaftAppBridge {
    pub fn new(config: BridgeConfig) -> Self {
        Self { config }
    }
    async fn connect(config: BridgeConfig) -> anyhow::Result<Channel> {
        let chan = match config {
            BridgeConfig::IPSocket(addr) => {
                let endpoint = Endpoint::try_from(format!("http://{}", addr))?;
                endpoint.connect().await?
            }
            BridgeConfig::UnixDomainSocket(path) => {
                // this address doesn't make sense because it won't be used
                // in the call of connect_with_connector.
                let endpoint = Endpoint::try_from("http://[::]:50051")?;
                endpoint
                    .connect_with_connector(service_fn(move |_: Uri| {
                        UnixStream::connect(path.clone())
                    }))
                    .await?
            }
        };
        Ok(chan)
    }
}
