use crate::*;

mod proto_compiled {
    tonic::include_proto!("fd");
}
use proto_compiled::{fd_server::Fd, Ping1Req, Ping2Rep, Ping2Req};

pub struct Server {
    pub uri: Uri,
    pub peer_out_cli: peer_out::ClientT,
}
#[tonic::async_trait]
impl Fd for Server {
    async fn ping1(
        &self,
        _: tonic::Request<Ping1Req>,
    ) -> Result<tonic::Response<()>, tonic::Status> {
        Ok(tonic::Response::new(()))
    }
    async fn ping2(
        &self,
        req: tonic::Request<Ping2Req>,
    ) -> Result<tonic::Response<Ping2Rep>, tonic::Status> {
        let req = req.into_inner();
        let suspect = req.suspect_uri;
        let suspect: Uri = suspect.parse().unwrap();
        let mut peer_out_cli = self.peer_out_cli.clone();
        let ok = peer_out_cli.ping1(suspect).await;
        let rep = Ping2Rep { ok };
        Ok(tonic::Response::new(rep))
    }
}
pub async fn make_service(server: Server) -> proto_compiled::fd_server::FdServer<Server> {
    proto_compiled::fd_server::FdServer::new(server)
}
