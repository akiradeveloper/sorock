use integration_tests::kvs::*;
use integration_tests::cluster::*;

use std::thread;
use std::time::Duration;

#[test]
fn test_reelection_large_cluster() {
    let n = 8;
    let env = init_cluster(n);
    thread::sleep(Duration::from_secs(10));
    env.stop(0);
    thread::sleep(Duration::from_millis(500));
    ensure_membership(
        Duration::from_secs(5),
        (1..n).collect(),
        (0..n).collect(),
        env.clone(),
    )
}
#[test]
fn test_reelection_after_leader_crash() {
    let env = init_cluster(3);

    // kill leader
    env.stop(0);
    // ND1 or ND2 becomes a new leader
    thread::sleep(Duration::from_secs(5));
    ensure_membership(
        Duration::from_secs(5),
        vec![1, 2],
        vec![0, 1, 2],
        env.clone(),
    );
}
#[test]
fn test_two_nodes_up_after_down() {
    let env = init_cluster(3);

    Client::to(0, env.clone()).set("k", "1").unwrap();
    env.stop(0);
    env.stop(1);

    env.start(0, kvs_server(vec![]));
    env.start(1, kvs_server(vec![]));
    thread::sleep(Duration::from_secs(5));

    let v = Client::to(2, env.clone()).get("k").unwrap().0;
    assert_eq!(v, Some("1".to_owned()));
}
#[test]
fn test_reelection_after_leader_stepdown() {
    let env = init_cluster(3);

    Admin::to(0, env.clone()).remove_server(0).unwrap();
    thread::sleep(Duration::from_secs(5));
    ensure_membership(Duration::from_secs(5), vec![1, 2], vec![1, 2], env.clone());

    Admin::to(1, env.clone()).add_server(0).unwrap();
    ensure_membership(
        Duration::from_secs(5),
        vec![0, 1, 2],
        vec![0, 1, 2],
        env.clone(),
    );
}
#[test]
fn test_timeout_now() {
    let env = init_cluster(3);
    let cluster_info = Admin::to(0, env.clone()).cluster_info().unwrap();
    assert_eq!(cluster_info.leader_id, Some(env.get_node_id(0)));

    Admin::to(2, env.clone()).timeout_now().unwrap();
    thread::sleep(Duration::from_secs(2));
    let cluster_info = Admin::to(0, env.clone()).cluster_info().unwrap();
    assert_eq!(cluster_info.leader_id, Some(env.get_node_id(2)));
}
#[test]
fn test_yield_leadership() {
    let env = init_cluster(3);
    let cluster_info = Admin::to(0, env.clone()).cluster_info().unwrap();
    assert_eq!(cluster_info.leader_id, Some(env.get_node_id(0)));

    Admin::to(0, env.clone())
        .remove_server(0)
        .unwrap();
    thread::sleep(Duration::from_millis(100));
    let cluster_info = Admin::to(1, env.clone()).cluster_info().unwrap();
    assert!(cluster_info.leader_id.is_some());
    assert_ne!(cluster_info.leader_id, Some(env.get_node_id(0)));
}