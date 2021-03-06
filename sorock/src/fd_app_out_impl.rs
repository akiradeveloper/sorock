use failure_detector as FD;
use std::sync::Arc;
use tonic::transport::{Channel, Uri};
use FD::app_out as M;

pub fn spawn(state: State) -> M::ClientT {
    use norpc::runtime::tokio::*;
    let svc = App {
        state: state.into(),
    };
    let svc = M::AppOutService::new(svc);
    let (chan, server) = ServerBuilder::new(svc).build();
    tokio::spawn(server.serve());
    M::AppOutClient::new(chan)
}

pub struct State {
    chan: Channel,
}
impl State {
    pub fn new(uri: Uri) -> Self {
        let e = tonic::transport::Endpoint::new(uri).unwrap();
        let chan = e.connect_lazy();
        Self { chan }
    }
}

#[derive(Clone)]
struct App {
    state: Arc<State>,
}

#[norpc::async_trait]
impl M::AppOut for App {
    async fn notify_failure(&self, culprit: Uri) -> anyhow::Result<()> {
        eprintln!("{} is failed.", culprit);

        let mut cli1 =
            sorock_core::proto_compiled::sorock_client::SorockClient::new(self.state.chan.clone());
        cli1.remove_node(sorock_core::proto_compiled::RemoveNodeReq {
            uri: culprit.to_string(),
        })
        .await?;

        let mut cli2 = lol_core::RaftClient::new(self.state.chan.clone());
        cli2.remove_server(lol_core::api::RemoveServerReq {
            id: culprit.to_string(),
        })
        .await?;

        Ok(())
    }
}
