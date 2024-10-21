use anyhow::Result;
use bytes::Bytes;
use sorock::service::raft::client::*;
use tonic::codegen::CompressionEncoding;
use tonic::transport::Channel;

pub mod ping_app;
pub mod raft_process;

#[derive(serde::Serialize, serde::Deserialize)]
pub enum AppWriteRequest {
    FetchAdd { bytes: Vec<u8> },
}
impl AppWriteRequest {
    pub fn serialize(self) -> Bytes {
        bincode::serialize(&self).unwrap().into()
    }

    pub fn deserialize(bytes: &[u8]) -> Self {
        bincode::deserialize(bytes).unwrap()
    }
}

#[derive(serde::Serialize, serde::Deserialize)]
pub enum AppReadRequest {
    Read,
    MakeSnapshot,
}
impl AppReadRequest {
    pub fn serialize(self) -> Bytes {
        bincode::serialize(&self).unwrap().into()
    }

    pub fn deserialize(bytes: &[u8]) -> Self {
        bincode::deserialize(bytes).unwrap()
    }
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Copy, Debug)]
pub struct AppState(pub u64);
impl AppState {
    pub fn serialize(&self) -> Bytes {
        bincode::serialize(&self).unwrap().into()
    }

    pub fn deserialize(bytes: &[u8]) -> Self {
        bincode::deserialize(bytes).unwrap()
    }
}

pub struct Client {
    cli: RaftClient,
}
impl Client {
    pub fn new(conn: Channel) -> Self {
        let cli = RaftClient::new(conn)
            .send_compressed(CompressionEncoding::Zstd)
            .accept_compressed(CompressionEncoding::Zstd);
        Self { cli }
    }

    pub async fn fetch_add(&mut self, shard_id: u32, n: u64) -> Result<u64> {
        let request_id = uuid::Uuid::new_v4().to_string();
        let req = WriteRequest {
            shard_id,
            message: AppWriteRequest::FetchAdd {
                bytes: vec![1u8; n as usize].into(),
            }
            .serialize(),
            request_id,
        };

        use tokio_retry::strategy::ExponentialBackoff;
        use tokio_retry::Retry;

        // 200ms, 400, 800, 1600, 3200, ...
        let strategy = ExponentialBackoff::from_millis(2).factor(100).take(8);

        let fut = Retry::spawn(strategy, || {
            let mut cli = self.cli.clone();
            let req = req.clone();
            async move { cli.write(req).await }
        });

        let resp = fut.await?.into_inner();
        let resp = AppState::deserialize(&resp.message);
        Ok(resp.0)
    }

    pub async fn read(&self, shard_id: u32) -> Result<u64> {
        let req = ReadRequest {
            shard_id,
            message: AppReadRequest::Read.serialize(),
            read_locally: false,
        };
        let resp = self.cli.clone().read(req).await?.into_inner();
        let resp = AppState::deserialize(&resp.message);
        Ok(resp.0)
    }

    pub async fn make_snapshot(&self, shard_id: u32) -> Result<u64> {
        let req = ReadRequest {
            shard_id,
            message: AppReadRequest::MakeSnapshot.serialize(),
            read_locally: true,
        };
        let resp = self.cli.clone().read(req).await?.into_inner();
        let resp = AppState::deserialize(&resp.message);
        Ok(resp.0)
    }
}
