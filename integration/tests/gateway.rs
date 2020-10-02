use integration::*;
use lol_core::connection::{gateway, Connection, EndpointConfig};
use std::collections::HashSet;
use std::time::Duration;

#[tokio::test(threaded_scheduler)]
async fn test_gateway() {
    let env = init_cluster(1);
    let mut initial = HashSet::new();
    initial.insert(env.node_id(0).to_owned());
    let gateway = gateway::Gateway::new(initial).await;
    gateway::Gateway::start_companion_thread(&gateway).await;

    env.start(1, vec![]);
    env.start(2, vec![]);
    Admin::to(0, env.clone()).add_server(1, env.clone());
    Admin::to(0, env.clone()).add_server(2, env.clone());
    tokio::time::delay_for(Duration::from_secs(6)).await;

    let config = EndpointConfig::default();
    let endpoints = gateway.read().await.query_sequence();
    assert_eq!(endpoints.len(), 3);
    let r = gateway::exec(&config, &endpoints, |_: Connection| async { Ok(()) }).await;
    assert!(r.is_ok());

    env.stop(0);
    env.stop(1);
    tokio::time::delay_for(Duration::from_secs(1)).await;

    let endpoints = gateway.read().await.query_sequence();
    let r = gateway::exec(&config, &endpoints, |_: Connection| async { Ok(()) }).await;
    // id=2 is alive
    assert!(r.is_ok());

    env.stop(2);
    tokio::time::delay_for(Duration::from_secs(1)).await;

    let endpoints = gateway.read().await.query_sequence();
    let r = gateway::exec(&config, &endpoints, |_: Connection| async { Ok(()) }).await;
    // all nodes are down
    assert!(r.is_err());
}
