use anyhow::Result;
use lol_tests::*;
use serial_test::serial;
use test_log::test;

#[serial]
#[test(tokio::test(flavor = "multi_thread"))]
async fn cluster_1() -> Result<()> {
    let mut cluster = Cluster::new(1).await?;
    cluster.add_server(0, 0).await?;
    Ok(())
}

#[serial]
#[test(tokio::test(flavor = "multi_thread"))]
async fn write_1() -> Result<()> {
    let mut cluster = Cluster::new(1).await?;
    cluster.add_server(0, 0).await?;

    cluster.try_commit(0).await?;
    let mut cli = cluster.user(0);
    assert_eq!(cli.fetch_add(1).await?, 0);
    assert_eq!(cli.fetch_add(2).await?, 1);
    assert_eq!(cli.fetch_add(3).await?, 3);

    Ok(())
}

#[serial]
#[test(tokio::test(flavor = "multi_thread"))]
async fn read_1() -> Result<()> {
    let mut cluster = Cluster::new(1).await?;
    cluster.add_server(0, 0).await?;

    cluster.try_commit(0).await?;
    let mut cli = cluster.user(0);
    assert_eq!(cli.read().await?, 0);
    assert_eq!(cli.fetch_add(1).await?, 0);
    assert_eq!(cli.read().await?, 1);
    assert_eq!(cli.fetch_add(2).await?, 1);
    assert_eq!(cli.read().await?, 3);

    Ok(())
}

#[serial]
#[test(tokio::test(flavor = "multi_thread"))]
async fn snapshot_1() -> Result<()> {
    let mut cluster = Cluster::new(1).await?;
    cluster.add_server(0, 0).await?;

    cluster.try_commit(0).await?;
    for n in 1..10 {
        cluster.user(0).fetch_add(n).await?;
    }

    cluster.user(0).make_snapshot().await?;

    for n in 1..10 {
        cluster.user(0).fetch_add(n).await?;
    }

    Ok(())
}
