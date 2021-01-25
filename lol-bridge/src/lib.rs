use async_trait::async_trait;
use lol_core::compat::RaftAppCompat;
use lol_core::Index;
use std::convert::TryFrom;
use tonic::transport::{Channel, Endpoint, Uri};

#[cfg(unix)]
use tokio::net::UnixStream;
use tower::service_fn;

mod proto_compiled {
    tonic::include_proto!("lol_bridge");
}
use proto_compiled::app_bridge_client::AppBridgeClient;

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
impl RaftAppCompat for RaftAppBridge {
    async fn process_message(&self, request: &[u8]) -> anyhow::Result<Vec<u8>> {
        let chan = Self::connect(self.config.clone()).await?;
        let mut cli = AppBridgeClient::new(chan);
        let req = proto_compiled::ProcessMessageReq {
            message: request.to_vec(),
        };
        let proto_compiled::ProcessMessageRep { message } =
            cli.process_message(req).await?.into_inner();
        Ok(message)
    }
    async fn apply_message(
        &self,
        request: &[u8],
        apply_index: Index,
    ) -> anyhow::Result<(Vec<u8>, Option<Vec<u8>>)> {
        let chan = Self::connect(self.config.clone()).await?;
        let mut cli = AppBridgeClient::new(chan);
        let req = proto_compiled::ApplyMessageReq {
            message: request.to_vec(),
            apply_index,
        };
        let proto_compiled::ApplyMessageRep { message, snapshot } =
            cli.apply_message(req).await?.into_inner();
        Ok((message, snapshot))
    }
    async fn install_snapshot(
        &self,
        snapshot: Option<&[u8]>,
        apply_index: Index,
    ) -> anyhow::Result<()> {
        let chan = Self::connect(self.config.clone()).await?;
        let mut cli = AppBridgeClient::new(chan);
        let req = proto_compiled::InstallSnapshotReq {
            snapshot: snapshot.map(|x| x.as_ref().to_vec()),
            apply_index,
        };
        cli.install_snapshot(req).await?;
        Ok(())
    }
    async fn fold_snapshot(
        &self,
        old_snapshot: Option<&[u8]>,
        requests: Vec<&[u8]>,
    ) -> anyhow::Result<Vec<u8>> {
        let chan = Self::connect(self.config.clone()).await?;
        let mut cli = AppBridgeClient::new(chan);
        let req = proto_compiled::FoldSnapshotReq {
            snapshot: old_snapshot.map(|x| x.as_ref().to_vec()),
            messages: requests.into_iter().map(|x| x.to_vec()).collect(),
        };
        let proto_compiled::FoldSnapshotRep { snapshot } =
            cli.fold_snapshot(req).await?.into_inner();
        Ok(snapshot)
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
