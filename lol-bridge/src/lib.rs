use async_trait::async_trait;
use lol_core::{SnapshotTag, RaftApp, Index};
use lol_core::snapshot::{SnapshotStream, BytesSnapshot};
use std::convert::TryFrom;
use tonic::transport::{Channel, Endpoint, Uri};
use bytes::Bytes;

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
    async fn process_message(&self, request: &[u8]) -> anyhow::Result<Vec<u8>> {
        let chan = Self::connect(self.config.clone()).await?;
        let mut cli = AppBridgeClient::new(chan);
        let req = protoimpl::ProcessMessageReq { message: request.to_vec() };
        let protoimpl::ProcessMessageRep { message } = cli.process_message(req).await?.into_inner();
        Ok(message)
    }
    async fn apply_message(&self, request: &[u8], apply_index: Index) -> anyhow::Result<(Vec<u8>, Option<SnapshotTag>)> {
        let chan = Self::connect(self.config.clone()).await?;
        let mut cli = AppBridgeClient::new(chan);
        let req = protoimpl::ApplyMessageReq { message: request.to_vec(), apply_index, };
        let protoimpl::ApplyMessageRep { message, snapshot } = cli.apply_message(req).await?.into_inner();
        let snapshot = snapshot.map(|x| x.into());
        Ok((message, snapshot))
    }
    async fn install_snapshot(&self, snapshot: Option<SnapshotTag>, apply_index: Index) -> anyhow::Result<()> {
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
        old_snapshot: Option<SnapshotTag>,
        requests: Vec<&[u8]>,
    ) -> anyhow::Result<SnapshotTag> {
        let chan = Self::connect(self.config.clone()).await?;
        let mut cli = AppBridgeClient::new(chan);
        let req = protoimpl::FoldSnapshotReq {
            snapshot: old_snapshot.map(|x| x.as_ref().to_vec()),
            messages: requests.into_iter().map(|x| x.to_vec()).collect(),
        };
        let protoimpl::FoldSnapshotRep { snapshot } = cli.fold_snapshot(req).await?.into_inner();
        Ok(snapshot.into())
    }
    async fn from_snapshot_stream(&self, st: SnapshotStream) -> anyhow::Result<SnapshotTag> {
        let b = BytesSnapshot::from_snapshot_stream(st).await?;
        let b = b.as_ref().to_vec();
        Ok(b.into())
    }
    async fn to_snapshot_stream(&self, x: SnapshotTag) -> SnapshotStream {
        let b: BytesSnapshot = x.as_ref().to_vec().into();
        b.to_snapshot_stream().await
    }
    async fn delete_resource(&self, _: SnapshotTag) -> anyhow::Result<()> {
        Ok(())
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
