use integration_tests::cluster::*;
use integration_tests::env::*;
use integration_tests::kvs::*;

use std::thread;
use std::time::Duration;

#[test]
fn test_one_node_start_and_stop() {
    let env = init_cluster(1);
    for _ in 0..10 {
        env.start(1, kvs_server(vec![]));
        env.stop(1);
    }
}
#[test]
fn test_pause() {
    let env = init_cluster(1);
    let r = Client::to(0, env.clone()).set("a", "1");
    assert!(r.is_ok());

    env.pause(0);
    let r = Client::to(0, env.clone()).set("a", "1");
    // timeout
    assert!(r.is_err());

    env.unpause(0);
    let r = Client::to(0, env.clone()).set("a", "1");
    assert!(r.is_ok());
}
#[test]
fn test_add_existing_node() {
    let env = init_cluster(1);
    for _ in 0..100 {
        Admin::to(0, env.clone()).add_server(0).unwrap();
    }
    ensure_membership(Duration::from_secs(5), vec![0], vec![0], env.clone())
}
#[test]
fn test_env_drop() {
    for _ in 0..10 {
        let env = init_cluster(1);
        for id in 1..=100 {
            env.start(id, kvs_server(vec![]));
        }
    }
}
#[test]
fn test_create_three_nodes_cluster() {
    let env = init_cluster(1);
    ensure_membership(Duration::from_secs(5), vec![0], vec![0], env.clone());

    env.start(1, kvs_server(vec![]));

    Admin::to(0, env.clone()).add_server(1).unwrap();
    ensure_membership(Duration::from_secs(5), vec![0, 1], vec![0, 1], env.clone());

    env.start(2, kvs_server(vec![]));

    Admin::to(1, env.clone()).add_server(2).unwrap();
    ensure_membership(
        Duration::from_secs(5),
        vec![0, 1, 2],
        vec![0, 1, 2],
        env.clone(),
    );
}
#[test]
fn test_init_cluster() {
    init_cluster(100);
}
#[test]
fn test_init_cluster_again() {
    let env = init_cluster(1);
    Admin::to(0, env.clone()).remove_server(0).unwrap();
    Admin::to(0, env.clone()).add_server(0).unwrap();
    ensure_membership(Duration::from_secs(5), vec![0], vec![0], env.clone());
}
#[test]
fn test_one_node_operations() {
    let env = init_cluster(1);
    let x = Client::to(0, env.clone()).get("a").unwrap();
    assert!(x.0.is_none());
    Client::to(0, env.clone()).set("a", "1").unwrap();
    let x = Client::to(0, env.clone()).get("a").unwrap();
    assert_eq!(x.0, Some("1".to_owned()));
}
#[test]
fn test_two_nodes_operations() {
    let env = init_cluster(1);

    env.start(1, kvs_server(vec![]));

    Admin::to(0, env.clone()).add_server(1).unwrap();
    ensure_membership(Duration::from_secs(5), vec![0, 1], vec![0, 1], env.clone());

    Client::to(0, env.clone()).set("a", "1").unwrap();
    Client::to(1, env.clone()).set("b", "1").unwrap();
}
#[test]
fn test_three_nodes_operations() {
    let env = init_cluster(3);

    let x = Client::to(1, env.clone()).get("a").unwrap();
    assert!(x.0.is_none());
    Client::to(2, env.clone()).set("a", "1").unwrap();
    let x = Client::to(1, env.clone()).get("a").unwrap();
    assert_eq!(x.0, Some("1".to_owned()));
}
#[test]
fn test_16_nodes_operations() {
    let env = init_cluster(16);
    for i in 1..100 {
        let x = format!("{}", i);
        Client::to(0, env.clone()).set(&x, &x).unwrap();
        assert!(eventually(Duration::from_secs(5), Some(x.clone()), || {
            Client::to(0, env.clone()).get(&x).unwrap().0
        }))
    }
}
#[test]
fn test_two_down_consensus_failure() {
    let env = init_cluster(3);

    env.stop(1);
    env.stop(2);

    let r = Client::to(0, env.clone()).set("a", "1");
    // timeout
    assert!(r.is_err());
}
#[test]
fn test_one_down_consensus_success() {
    let env = init_cluster(3);

    // kill leader
    env.stop(0);

    // ND1 or ND2 becomes leader
    thread::sleep(Duration::from_secs(5));

    let r = Client::to(1, env.clone()).set("a", "1");
    assert!(r.is_ok());
}
#[test]
fn test_forwarding_err() {
    let env = init_cluster(3);

    // kill leader
    env.stop(0);

    // sending request to ND1 and then forward to ND0 but fail
    let r = Client::to(1, env.clone()).set("a", "1");
    assert!(r.is_err());
}
#[test]
fn test_slow_node_catch_up() {
    let env = init_cluster(3);
    let e1 = env.clone();
    let client = thread::spawn(move || {
        for _ in 0..2000 {
            Client::to(0, e1.clone()).set("a", "1").unwrap();
        }
    });
    for _ in 0..4 {
        thread::sleep(Duration::from_secs(5));
        env.pause(2);
        thread::sleep(Duration::from_secs(5));
        env.unpause(2);
    }
    client.join().unwrap();
}
#[test]
fn test_add_new_node() {
    let env = init_cluster(2);
    for _ in 1..1000 {
        Client::to(0, env.clone()).set("a", "1").unwrap();
    }
    thread::sleep(Duration::from_secs(10));
    env.start(2, kvs_server(vec![]));
    Admin::to(0, env.clone()).add_server(2).unwrap();
    thread::sleep(Duration::from_secs(5));
    env.stop(1);
    let r = Client::to(0, env.clone()).get("a");
    assert!(r.is_ok());
}
#[test]
fn test_replicate_fast_snapshot_to_slow_node() {
    let env = init_cluster(3);
    for _ in 1..500 {
        Client::to(0, env.clone()).set("a", "1").unwrap();
    }

    // ND2 can't receive entries
    env.pause(2);
    for _ in 1..500 {
        Client::to(0, env.clone()).set("a", "1").unwrap();
    }

    // wait for a new snapshot to be made
    thread::sleep(Duration::from_secs(10));

    env.stop(1);

    // new snapshot is replicated to ND2
    env.unpause(2);
    thread::sleep(Duration::from_secs(2));

    // ND0 and ND2 alone can make a consensus
    let r = Client::to(0, env.clone()).get("a");
    assert!(r.is_ok());
}
#[test]
fn test_huge_replication() {
    let env = init_cluster(3);
    // 10^5
    let mut s = String::new();
    for _ in 0..100000 {
        s.push('a');
    }
    // 10^8
    let r = Client::to(0, env.clone()).set_rep("k", &s, 1000);
    assert!(r.is_ok());

    env.stop(0);
    // wait for compaction
    thread::sleep(Duration::from_secs(10));

    let r = Client::to(0, env.clone()).get("k");
    assert!(r.is_err());

    env.stop(2);
    env.start(0, kvs_server(vec![]));

    // wait for reelection in case the former leader is ND2
    thread::sleep(Duration::from_secs(5));

    // after huge replication, read will succeed
    let x = Client::to(1, env.clone()).get("k").expect("read failed");
    assert_eq!(x.0.unwrap().len(), 100_000_000);
}
#[test]
fn test_huge_request() {
    let env = init_cluster(1);
    // 10^5
    let mut s = String::new();
    for _ in 0..100000 {
        s.push('a');
    }
    let r = Client::to(0, env.clone()).set_rep("k1", &s, 1); // 10^5
    assert!(r.is_ok());
    let r = Client::to(0, env.clone()).set_rep("k2", &s, 2); // 2 * 10^5
    assert!(r.is_ok());
    let r = Client::to(0, env.clone()).set_rep("k3", &s, 10); // 10^6
    assert!(r.is_ok());
    let r = Client::to(0, env.clone()).set_rep("k4", &s, 100); // 10^7
    assert!(r.is_ok());
    let r = Client::to(0, env.clone()).set_rep("k5", &s, 1000); // 10^8
    assert!(r.is_ok());
    let x = Client::to(0, env.clone()).get("k5").unwrap();
    assert_eq!(x.0.unwrap().len(), 100_000_000);
}
#[test]
fn test_copy_snapshot() {
    let env = env_new(0, kvs_server(vec!["--copy-snapshot-mode"]));
    Client::to(0, env.clone()).set_rep("k", "1", 1).unwrap();
    Client::to(0, env.clone()).set_rep("k", "2", 1).unwrap();
    Client::to(0, env.clone()).set_rep("k", "3", 1).unwrap();
    Client::to(0, env.clone()).set_rep("k", "2", 1).unwrap();
    // snapshot inserted
    thread::sleep(Duration::from_secs(3));

    // add ND1 into the cluster
    env.start(1, kvs_server(vec![]));
    Admin::to(0, env.clone()).add_server(1).unwrap();
    // snapshot is replicated
    thread::sleep(Duration::from_secs(3));

    env.stop(0);
    env.start(0, kvs_server(vec![]));
    // wait for reelection
    thread::sleep(Duration::from_secs(3));

    let v = Client::to(1, env.clone()).get("k").unwrap().0.unwrap();
    assert_eq!(v, "2");
}
