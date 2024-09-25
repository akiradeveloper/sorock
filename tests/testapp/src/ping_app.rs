use tracing::info;

mod proto {
    tonic::include_proto!("ping");
}

pub use proto::ping_client::PingClient;

pub fn new_service() -> proto::ping_server::PingServer<impl proto::ping_server::Ping> {
    proto::ping_server::PingServer::new(PingApp)
}

struct PingApp;
#[tonic::async_trait]
impl proto::ping_server::Ping for PingApp {
    async fn ping(
        &self,
        _: tonic::Request<()>,
    ) -> std::result::Result<tonic::Response<()>, tonic::Status> {
        info!("ping");
        Ok(tonic::Response::new(()))
    }

    async fn panic(
        &self,
        _: tonic::Request<()>,
    ) -> std::result::Result<tonic::Response<()>, tonic::Status> {
        info!("panic");
        panic!()
    }
}
