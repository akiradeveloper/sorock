use anyhow::Result;
use lol_tests::*;
use serial_test::serial;

#[serial]
#[tokio::test(flavor = "multi_thread")]
async fn n5_cluster() -> Result<()> {
    let mut cluster = Cluster::new(5, 1).await?;
    cluster.add_server(0, 0, 0).await?;
    cluster.add_server(0, 0, 1).await?;
    cluster.add_server(0, 1, 2).await?;
    cluster.add_server(0, 2, 3).await?;
    cluster.add_server(0, 3, 4).await?;
    Ok(())
}
