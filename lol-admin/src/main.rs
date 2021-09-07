use clap::Clap;
use lol_core::{core_message, proto_compiled, proto_compiled::raft_client::RaftClient};
use std::time::Duration;
use tonic::transport::channel::Endpoint;

#[derive(Clap, Debug)]
#[clap(name = "lol-admin")]
struct Opts {
    #[clap(name = "DEST_ID")]
    dest_id: String,
    #[clap(subcommand)]
    sub: Sub,
}
#[derive(Clap, Debug)]
enum Sub {
    #[clap(name = "add-server")]
    AddServer {
        #[clap(name = "ID")]
        id: String,
    },
    #[clap(name = "remove-server")]
    RemoveServer {
        #[clap(name = "ID")]
        id: String,
    },
    #[clap(name = "cluster-info")]
    ClusterInfo,
    #[clap(name = "timeout-now")]
    TimeoutNow,
    #[clap(name = "tunable-config")]
    TunableConfigInfo,
}
#[tokio::main]
async fn main() {
    let opt = Opts::parse();
    let endpoint = Endpoint::from_shared(opt.dest_id)
        .unwrap()
        .timeout(Duration::from_secs(5));
    let mut conn = RaftClient::connect(endpoint).await.unwrap();
    match opt.sub {
        Sub::AddServer { id } => {
            let req = proto_compiled::AddServerReq { id };
            conn.add_server(req).await.unwrap();
        }
        Sub::RemoveServer { id } => {
            let req = proto_compiled::RemoveServerReq { id };
            conn.remove_server(req).await.unwrap();
        }
        Sub::ClusterInfo => {
            let req = proto_compiled::ClusterInfoReq {};
            let rep = conn.request_cluster_info(req).await.unwrap().into_inner();
            let info = lol_admin::ClusterInfo {
                leader_id: rep.leader_id,
                membership: rep.membership,
            };
            let json = serde_json::to_string(&info).unwrap();
            println!("{}", json);
        }
        Sub::TimeoutNow => {
            let req = proto_compiled::TimeoutNowReq {};
            conn.timeout_now(req).await.unwrap();
        }
        Sub::TunableConfigInfo => {
            let msg = core_message::Req::TunableConfigInfo;
            let req = proto_compiled::ProcessReq {
                message: core_message::Req::serialize(&msg),
                core: true,
            };
            let res = conn
                .request_process_locally(req)
                .await
                .unwrap()
                .into_inner();
            let msg = core_message::Rep::deserialize(&res.message).unwrap();
            println!("{}", serde_json::to_string(&msg).unwrap())
        }
    }
}
