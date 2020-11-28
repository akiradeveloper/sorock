use lol_core::{core_message, proto_compiled};
use std::time::Duration;
use structopt::StructOpt;

#[derive(StructOpt, Debug)]
#[structopt(name = "lol-admin")]
struct Opt {
    #[structopt(name = "DEST_ID")]
    dest_id: String,
    #[structopt(subcommand)]
    sub: Sub,
}
#[derive(Debug, StructOpt)]
enum Sub {
    #[structopt(name = "init-cluster")]
    InitCluster,
    #[structopt(name = "add-server")]
    AddServer {
        #[structopt(name = "ID")]
        id: String,
    },
    #[structopt(name = "remove-server")]
    RemoveServer {
        #[structopt(name = "ID")]
        id: String,
    },
    #[structopt(name = "cluster-info")]
    ClusterInfo,
    #[structopt(name = "timeout-now")]
    TimeoutNow,
}
#[tokio::main]
async fn main() {
    let opt = Opt::from_args();
    let endpoint = lol_core::connection::Endpoint::from_shared(opt.dest_id).unwrap().timeout(Duration::from_secs(5));
    let mut conn = lol_core::connection::connect(endpoint).await.unwrap();
    match opt.sub {
        Sub::AddServer { id } => {
            let req = proto_compiled::AddServerReq { id, };
            conn.add_server(req).await.unwrap();
        }
        Sub::RemoveServer { id } => {
            let req = proto_compiled::RemoveServerReq { id, };
            conn.remove_server(req).await.unwrap();
        }
        // will be removed.
        Sub::InitCluster => {
            let msg = core_message::Req::InitCluster;
            let req = proto_compiled::ProcessReq {
                message: core_message::Req::serialize(&msg),
                core: true,
            };
            let res = conn.request_process_locally(req).await.unwrap().into_inner();
            let msg = core_message::Rep::deserialize(&res.message).unwrap();
            let msg = if let core_message::Rep::InitCluster { ok } = msg {
                lol_admin::InitCluster { ok }
            } else {
                unreachable!()
            };
            let json = serde_json::to_string(&msg).unwrap();
            println!("{}", json);
        }
        // core locally
        Sub::ClusterInfo => {
            let msg = core_message::Req::ClusterInfo;
            let req = proto_compiled::ProcessReq {
                message: core_message::Req::serialize(&msg),
                core: true,
            };
            let res = conn.request_process_locally(req).await.unwrap().into_inner();
            let msg = core_message::Rep::deserialize(&res.message).unwrap();
            let msg = if let core_message::Rep::ClusterInfo {
                leader_id,
                membership,
            } = msg
            {
                lol_admin::ClusterInfo {
                    leader_id,
                    membership,
                }
            } else {
                unreachable!()
            };
            let json = serde_json::to_string(&msg).unwrap();
            println!("{}", json);
        }
        Sub::TimeoutNow => {
            let req = proto_compiled::TimeoutNowReq {};
            conn.timeout_now(req).await.unwrap();
        }
    }
}
