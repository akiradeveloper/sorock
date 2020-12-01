use integration::*;
use lol_core::Id;
use lol_core::connection::{self, gateway};
use std::collections::HashSet;
use std::time::Duration;
use tonic::transport::channel::Endpoint;

#[tokio::test(threaded_scheduler)]
async fn test_gateway() {
    let env = init_cluster(1);
    let mut initial = HashSet::new();
    initial.insert(env.get_node_id(0).to_owned());
    let gateway = gateway::watch(initial);

    env.start(1, kvs_server(vec![]));
    env.start(2, kvs_server(vec![]));
    Admin::to(0, env.clone()).add_server(1);
    Admin::to(0, env.clone()).add_server(2);
    tokio::time::delay_for(Duration::from_secs(6)).await;

    let connect = |id| async {
        let endpoint = Endpoint::from_shared(id).unwrap();
        connection::connect(endpoint).await?;
        Ok(())
    };

    let endpoints = gateway.borrow().list.clone();
    assert_eq!(endpoints.len(), 3);
    let r = gateway::exec(endpoints, |id: Id| connect(id)).await;
    assert!(r.is_ok());

    env.stop(0);
    env.stop(1);
    tokio::time::delay_for(Duration::from_secs(1)).await;

    let endpoints = gateway.borrow().list.clone();
    let r = gateway::exec(endpoints, |id: Id| connect(id)).await;
    // id=2 is alive
    assert!(r.is_ok());

    env.stop(2);
    tokio::time::delay_for(Duration::from_secs(1)).await;

    let endpoints = gateway.borrow().list.clone();
    let r = gateway::exec(endpoints, |id: Id| connect(id)).await;
    // all nodes are down
    assert!(r.is_err());
}
