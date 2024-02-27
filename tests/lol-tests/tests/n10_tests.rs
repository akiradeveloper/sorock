use anyhow::Result;
use lol_tests::*;
use serial_test::serial;
use test_log::test;

#[serial]
#[test(tokio::test(flavor = "multi_thread"))]
async fn n10_cluster() -> Result<()> {
    let mut cluster = Cluster::new(3).await?;
    cluster.add_server(0, 0).await?;
    for i in 0..9 {
        cluster.add_server(i, i+1).await?;
    }
    Ok(())
}

#[serial]
#[test(tokio::test(flavor = "multi_thread"))]
async fn n10_write() -> Result<()> {
    let mut cluster = Cluster::new(3).await?;
    cluster.add_server(0, 0).await?;
    for i in 0..9 {
        cluster.add_server(i, i+1).await?;
    }
    
    let mut cur = 0;
    for i in (0..30).rev() {
        let k = 1<<i;
        let old = cluster.user(i%10).fetch_add(k).await?;
        assert_eq!(old, cur);
        cur += k;
    }
    Ok(())
}