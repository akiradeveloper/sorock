use anyhow::Result;
use rand::Rng;
use sorock_tests::*;
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<()> {
    console_subscriber::init();

    eprintln!("construct cluster");
    let mut cluster = Cluster::builder()
        .with_persistency(true)
        .with_logging(false)
        .build(3, 1)
        .await?;
    cluster.add_server(0, 0, 0).await?;
    cluster.add_server(0, 0, 1).await?;
    cluster.add_server(0, 1, 2).await?;

    eprintln!("cluster is ready. executing IO ...");
    let mut cur_state = 0;
    for i in 0..10 {
        let add_v = rand::thread_rng().gen_range(1..=9);
        let old_v = cluster.user(0).fetch_add(0, add_v).await?;
        assert_eq!(old_v, cur_state);
        cur_state += add_v;

        if i == 5 {
            cluster.user(0).make_snapshot(0).await?;
            cluster.user(1).make_snapshot(0).await?;
        }
    }

    eprintln!("shutdown cluster");
    cluster.env().remove_node(0);
    cluster.env().remove_node(1);
    cluster.env().remove_node(2);
    tokio::time::sleep(Duration::from_secs(1)).await;

    eprintln!("reboot ND0");
    cluster.env().add_node(0);
    cluster.env().check_connectivity(0).await?;
    eprintln!("reboot ND1");
    cluster.env().add_node(1);
    cluster.env().check_connectivity(1).await?;

    eprintln!("wait for election");
    tokio::time::sleep(Duration::from_secs(10)).await;

    eprintln!("read from ND1");
    loop {
        match cluster.user(1).read(0).await {
            Ok(v) => {
                assert_eq!(v, cur_state);
                eprintln!("OK");
                return Ok(());
            }
            Err(e) => {
                eprintln!("error: {:?}", e);
            }
        }
    }
}
