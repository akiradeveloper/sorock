use lol_core::{core_message, protoimpl};
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
    let dest_id = lol_core::connection::resolve(&opt.dest_id).unwrap();
    let endpoint = lol_core::connection::Endpoint::new(dest_id);
    let config = lol_core::connection::EndpointConfig::default().timeout(Duration::from_secs(5));
    let mut conn = endpoint.connect_with(config).await.unwrap();
    match opt.sub {
        // commit
        Sub::AddServer { id } => {
            let id = lol_core::connection::resolve(&id).unwrap();
            let msg = core_message::Req::AddServer(id);
            let req = protoimpl::CommitReq {
                message: core_message::Req::serialize(&msg),
                core: true,
            };
            conn.request_commit(req).await.unwrap();
        }
        // commit
        Sub::RemoveServer { id } => {
            let id = lol_core::connection::resolve(&id).unwrap();
            let msg = core_message::Req::RemoveServer(id);
            let req = protoimpl::CommitReq {
                message: core_message::Req::serialize(&msg),
                core: true,
            };
            conn.request_commit(req).await.unwrap();
        }
        // core locally
        Sub::InitCluster => {
            let msg = core_message::Req::InitCluster;
            let req = protoimpl::ProcessReq {
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
            let req = protoimpl::ProcessReq {
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
            let req = protoimpl::TimeoutNowReq {};
            conn.timeout_now(req).await.unwrap();
        }
    }
}
