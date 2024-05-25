use anyhow::Result;
use lol_tests::*;
use serial_test::serial;
use rand::Rng;

#[serial]
#[tokio::test(flavor = "multi_thread")]
async fn n10_cluster() -> Result<()> {
    let mut cluster = Cluster::new(10, 1).await?;
    cluster.add_server(0, 0, 0).await?;
    for i in 0..9 {
        cluster.add_server(0, i, i + 1).await?;
    }
    Ok(())
}

#[serial]
#[tokio::test(flavor = "multi_thread")]
async fn n10_write() -> Result<()> {
    let mut cluster = Cluster::new(10, 1).await?;
    cluster.add_server(0, 0, 0).await?;
    for i in 0..9 {
        cluster.add_server(0, i, i + 1).await?;
    }

    let mut cur_state = 0;
    for i in 0..100 {
        let add_v = rand::thread_rng().gen_range(1..=9);
        let io_node = (i % 10) as u8;
        let old_v = cluster.user(io_node).fetch_add(0, add_v).await?;
        assert_eq!(old_v, cur_state);
        cur_state += add_v;
    }

    Ok(())
}
