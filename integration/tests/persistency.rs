use integration::*;
use std::time::Duration;
use std::thread;

#[test]
fn test_persistency_reboot() {
    let env = Environment::new(0, vec!["--use-persistency=0", "--reset-persistency", "--compaction-interval-sec=0"]);
    for id in 1..=2 {
        let s = format!("--use-persistency={}", id);
        env.start(id, vec![&s, "--reset-persistency", "--compaction-interval-sec=0"]);
        thread::sleep(Duration::from_secs(2));
        Admin::to(0, env.clone()).add_server(id, env.clone());
    }
    // do some IOs
    Client::to(0, env.clone()).set("k", "1");
    Client::to(0, env.clone()).set("k", "2");
    Client::to(0, env.clone()).set("k", "3");

    // stop the servers
    env.stop(0);
    env.stop(1);
    env.stop(2);

    for id in 0..=2 {
        let s = format!("--use-persistency={}", id);
        env.start(id, vec![&s, "--compaction-interval-sec=0"]);
        thread::sleep(Duration::from_secs(2));
    } 
    thread::sleep(Duration::from_secs(5));
    // the servers should make a cluster and choose a new leader.

    let v = Client::to(2, env.clone()).get("k").unwrap().0.unwrap();
    assert_eq!(v, "3");
}