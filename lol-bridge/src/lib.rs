use async_trait::async_trait;
use lol_core::simple::RaftAppSimple;
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
impl RaftAppSimple for RaftAppBridge {
    async fn process_read(&self, request: &[u8]) -> anyhow::Result<Vec<u8>> {
        let chan = Self::connect(self.config.clone()).await?;
        let mut cli = AppBridgeClient::new(chan);
        let req = proto_compiled::ProcessReadReq {
            message: request.to_vec(),
        };
        let proto_compiled::ProcessReadRep { message } = cli.process_read(req).await?.into_inner();
        Ok(message)
    }
    async fn process_write(&self, request: &[u8]) -> anyhow::Result<(Vec<u8>, Option<Vec<u8>>)> {
        let chan = Self::connect(self.config.clone()).await?;
        let mut cli = AppBridgeClient::new(chan);
        let req = proto_compiled::ProcessWriteReq {
            message: request.to_vec(),
        };
        let proto_compiled::ProcessWriteRep { message, snapshot } =
            cli.process_write(req).await?.into_inner();
        Ok((message, snapshot))
    }
    async fn install_snapshot(&self, snapshot: Option<&[u8]>) -> anyhow::Result<()> {
        let chan = Self::connect(self.config.clone()).await?;
        let mut cli = AppBridgeClient::new(chan);
        let req = proto_compiled::InstallSnapshotReq {
            snapshot: snapshot.map(|x| x.as_ref().to_vec()),
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
