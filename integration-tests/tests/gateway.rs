use integration_tests::cluster::*;
use integration_tests::kvs::*;

use lol_core::connection::{self, gateway};
use lol_core::gateway as gateway_v2;
use lol_core::proto_compiled::{raft_client::RaftClient, ClusterInfoReq};
use lol_core::Id;
use std::collections::HashSet;
use std::time::Duration;
use tonic::transport::channel::Endpoint;

#[tokio::test(flavor = "multi_thread")]
async fn test_gateway_v2() {
    let env = init_cluster(1);
    let connector = gateway_v2::Connector::new(|id| Endpoint::from_shared(id).unwrap());
    let gateway = connector.connect(env.get_node_id(0));
    env.start(1, kvs_server(vec![]));
    env.start(2, kvs_server(vec![]));
    Admin::to(0, env.clone()).add_server(1).unwrap();
    Admin::to(0, env.clone()).add_server(2).unwrap();
    tokio::time::sleep(Duration::from_secs(6)).await;

    let mut cli1 = RaftClient::new(gateway.clone());
    let mut cli2 = RaftClient::new(gateway);
    let res = cli1.request_cluster_info(ClusterInfoReq {}).await;
    assert!(res.is_ok());
    let res = cli2.request_cluster_info(ClusterInfoReq {}).await;
    assert!(res.is_ok());

    // ND0をとめた時、リーダーがND1 or ND2に移る。
    // Gatewayはこれに追従出来る。
    env.stop(0);
    tokio::time::sleep(Duration::from_secs(2)).await;
    let res = cli1.request_cluster_info(ClusterInfoReq {}).await;
    assert!(res.is_ok());
}

#[tokio::test(flavor = "multi_thread")]
async fn test_gateway() {
    let env = init_cluster(1);
    let mut initial = HashSet::new();
    initial.insert(env.get_node_id(0).to_owned());
    let gateway = gateway::watch(initial);

    env.start(1, kvs_server(vec![]));
    env.start(2, kvs_server(vec![]));
    Admin::to(0, env.clone()).add_server(1).unwrap();
    Admin::to(0, env.clone()).add_server(2).unwrap();
    tokio::time::sleep(Duration::from_secs(6)).await;

    let connect = |id| async {
        let endpoint = Endpoint::from_shared(id).unwrap();
        RaftClient::connect(endpoint).await?;
        Ok(())
    };

    let endpoints = gateway.borrow().list.clone();
    assert_eq!(endpoints.len(), 3);
    let r = gateway::exec(endpoints, |id: Id| connect(id)).await;
    assert!(r.is_ok());

    env.stop(0);
    env.stop(1);
    tokio::time::sleep(Duration::from_secs(1)).await;

    let endpoints = gateway.borrow().list.clone();
    let r = gateway::exec(endpoints, |id: Id| connect(id)).await;
    // id=2 is alive
    assert!(r.is_ok());

    env.stop(2);
    tokio::time::sleep(Duration::from_secs(1)).await;

    let endpoints = gateway.borrow().list.clone();
    let r = gateway::exec(endpoints, |id: Id| connect(id)).await;
    // all nodes are down
    assert!(r.is_err());
}
