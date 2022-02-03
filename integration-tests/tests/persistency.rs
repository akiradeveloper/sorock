use integration_tests::cluster::*;
use integration_tests::kvs::*;

use std::thread;
use std::time::Duration;

#[test]
fn test_persistency_one_node_start() {
    let env = env_new(
        0,
        kvs_server(vec![
            "--use-persistency=0",
            "--reset-persistency",
            "--compaction-interval-sec=0",
        ]),
    );
    env.stop(0);
}

#[test]
fn test_persistency_membership() {
    let env = env_new(
        0,
        kvs_server(vec![
            "--use-persistency=0",
            "--reset-persistency",
            "--compaction-interval-sec=0",
        ]),
    );
    for id in 1..=2 {
        let s = format!("--use-persistency={}", id);
        env.start(
            id,
            kvs_server(vec![
                &s,
                "--reset-persistency",
                "--compaction-interval-sec=0",
            ]),
        );
        thread::sleep(Duration::from_secs(2));
        Admin::to(0, env.clone()).add_server(id).unwrap();
    }
    // do some IOs
    Client::to(0, env.clone()).set("k", "1").unwrap();
    Client::to(0, env.clone()).set("k", "2").unwrap();
    Client::to(0, env.clone()).set("k", "3").unwrap();

    // stop the servers
    env.stop(0);
    env.stop(1);
    env.stop(2);

    for id in 0..=2 {
        let s = format!("--use-persistency={}", id);
        env.start(id, kvs_server(vec![&s, "--compaction-interval-sec=0"]));
        thread::sleep(Duration::from_secs(2));
    }
    thread::sleep(Duration::from_secs(5));
    // the servers should make a cluster and choose a new leader.

    let v = Client::to(2, env.clone()).get("k").unwrap().0.unwrap();
    assert_eq!(v, "3");
}

#[test]
fn test_persistency_reboot() {
    let env = env_new(
        0,
        kvs_server(vec![
            "--use-persistency=0",
            "--reset-persistency",
            "--compaction-interval-sec=1",
        ]),
    );
    for _ in 0..10 {
        Client::to(0, env.clone()).set("k", "1").unwrap();
    }
    // wait for compaction and gc
    thread::sleep(Duration::from_secs(5));

    env.stop(0);
    env.start(
        0,
        kvs_server(vec!["--use-persistency=0", "--compaction-interval-sec=0"]),
    );
    // wait for boot, calculating commit_index
    thread::sleep(Duration::from_secs(5));

    let r = Client::to(0, env.clone()).set("k", "1");
    assert!(r.is_ok());
}
