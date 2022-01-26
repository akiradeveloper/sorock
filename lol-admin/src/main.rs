use clap::Parser;
use lol_core::{proto_compiled, proto_compiled::raft_client::RaftClient};
use std::time::Duration;
use tonic::transport::channel::Endpoint;

#[derive(Parser, Debug)]
#[clap(name = "lol-admin")]
struct Opts {
    #[clap(name = "DEST_ID")]
    dest_id: String,
    #[clap(subcommand)]
    sub: Sub,
}
#[derive(Parser, Debug)]
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
    #[clap(name = "status")]
    Status,
    #[clap(name = "config")]
    Config {
        #[clap(subcommand)]
        sub: ConfigSub,
    },
}
#[derive(Parser, Debug)]
enum ConfigSub {
    #[clap(name = "set")]
    Set {
        #[clap(long)]
        compaction_delay_sec: Option<u64>,
        #[clap(long)]
        compaction_interval_sec: Option<u64>,
    },
    #[clap(name = "get")]
    Get,
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
            let res = lol_admin::ClusterInfo {
                leader_id: rep.leader_id,
                membership: rep.membership,
            };
            println!("{}", serde_json::to_string(&res).unwrap());
        }
        Sub::TimeoutNow => {
            let req = proto_compiled::TimeoutNowReq {};
            conn.timeout_now(req).await.unwrap();
        }
        Sub::TunableConfigInfo => {
            let req = proto_compiled::GetConfigReq {};
            let rep = conn.get_config(req).await.unwrap().into_inner();
            let res = lol_admin::Config {
                compaction_delay_sec: rep.compaction_delay_sec,
                compaction_interval_sec: rep.compaction_interval_sec,
            };
            println!("{}", serde_json::to_string(&res).unwrap());
        }
        Sub::Status => {
            let req = proto_compiled::StatusReq {};
            let rep = conn.status(req).await.unwrap().into_inner();
            let res = lol_admin::Status {
                snapshot_index: rep.snapshot_index,
                last_applied: rep.last_applied,
                commit_index: rep.commit_index,
                last_log_index: rep.last_log_index,
            };
            println!("{}", serde_json::to_string(&res).unwrap());
        }
        Sub::Config { sub } => match sub {
            ConfigSub::Get => {
                let req = proto_compiled::GetConfigReq {};
                let rep = conn.get_config(req).await.unwrap().into_inner();
                let res = lol_admin::Config {
                    compaction_delay_sec: rep.compaction_delay_sec,
                    compaction_interval_sec: rep.compaction_interval_sec,
                };
                println!("{}", serde_json::to_string(&res).unwrap());
            }
            ConfigSub::Set {
                compaction_delay_sec,
                compaction_interval_sec,
            } => {
                let req = proto_compiled::TuneConfigReq {
                    compaction_delay_sec,
                    compaction_interval_sec,
                };
                conn.tune_config(req).await.unwrap();
            }
        },
    }
}
