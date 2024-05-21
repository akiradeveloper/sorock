use anyhow::Result;
use lol_tests::*;
use serial_test::serial;
use test_log::test;

#[serial]
#[test(tokio::test(flavor = "multi_thread"))]
async fn n10_cluster() -> Result<()> {
    let mut cluster = Cluster::new(10, 1).await?;
    cluster.add_server(0, 0, 0).await?;
    for i in 0..9 {
        cluster.add_server(0, i, i + 1).await?;
    }
    Ok(())
}

#[serial]
#[test(tokio::test(flavor = "multi_thread"))]
async fn n10_write() -> Result<()> {
    let mut cluster = Cluster::new(10, 1).await?;
    cluster.add_server(0, 0, 0).await?;
    for i in 0..9 {
        cluster.add_server(0, i, i + 1).await?;
    }

    let mut cur = 0;
    for i in (0..20).rev() {
        let k = 1 << i;
        let old = cluster.user(i % 10).fetch_add(0, k).await?;
        assert_eq!(old, cur);
        cur += k;
    }
    Ok(())
}
