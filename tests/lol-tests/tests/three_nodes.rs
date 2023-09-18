use anyhow::Result;
use lol_tests::*;
use serial_test::serial;
use test_log::test;

#[serial]
#[test(tokio::test(flavor = "multi_thread"))]
async fn cluster_3() -> Result<()> {
    let mut cluster = Cluster::new(3).await?;
    cluster.add_server(0, 0).await?;

    cluster.try_commit(0).await?;
    cluster.add_server(0, 1).await?;

    cluster.try_commit(1).await?;
    cluster.add_server(1, 2).await?;
    Ok(())
}

#[serial]
#[test(tokio::test(flavor = "multi_thread"))]
async fn write_3() -> Result<()> {
    let mut cluster = Cluster::new(3).await?;
    cluster.add_server(0, 0).await?;

    cluster.try_commit(0).await?;
    cluster.add_server(0, 1).await?;

    cluster.try_commit(1).await?;
    cluster.add_server(1, 2).await?;

    cluster.try_commit(2).await?;
    assert_eq!(cluster.user(2).fetch_add(1).await?, 0);
    assert_eq!(cluster.user(1).fetch_add(10).await?, 1);
    assert_eq!(cluster.user(0).fetch_add(100).await?, 11);
    assert_eq!(cluster.user(0).read().await?, 111);

    Ok(())
}

#[serial]
#[test(tokio::test(flavor = "multi_thread"))]
async fn snapshot_3() -> Result<()> {
    let mut cluster = Cluster::new(3).await?;
    cluster.add_server(0, 0).await?;

    cluster.try_commit(0).await?;
    cluster.user(0).fetch_add(1).await?;
    cluster.user(0).fetch_add(10).await?;
    cluster.user(0).fetch_add(100).await?;
    cluster.user(0).make_snapshot().await?;

    cluster.add_server(0, 1).await?;
    cluster.add_server(0, 2).await?;

    assert_eq!(cluster.user(0).fetch_add(1000).await?, 111);
    assert_eq!(cluster.user(0).fetch_add(10000).await?, 1111);

    Ok(())
}

#[serial]
#[test(tokio::test(flavor = "multi_thread"))]
async fn leader_stop_3() -> Result<()> {
    let mut cluster = Cluster::new(3).await?;
    cluster.add_server(0, 0).await?;

    cluster.try_commit(0).await?;
    for i in 0..10 {
        cluster.user(0).fetch_add(i).await?;
    }

    cluster.add_server(0, 1).await?;
    cluster.add_server(0, 2).await?;

    cluster.raw_env().stop(0).await?;
    tokio::time::sleep(tokio::time::Duration::from_secs(3)).await;

    cluster.try_commit(1).await?;
    for i in 0..10 {
        cluster.user(1).fetch_add(i).await?;
    }

    Ok(())
}

#[serial]
#[test(tokio::test(flavor = "multi_thread"))]
async fn leader_stepdown_3() -> Result<()> {
    let mut cluster = Cluster::new(3).await?;
    cluster.add_server(0, 0).await?;

    cluster.try_commit(0).await?;
    for i in 0..10 {
        cluster.user(0).fetch_add(i).await?;
    }

    cluster.add_server(0, 1).await?;
    cluster.add_server(0, 2).await?;

    cluster.try_commit(1).await?;
    cluster.remove_server(1, 0).await?;
    eprintln!("removed nd0 -> ok");
    tokio::time::sleep(tokio::time::Duration::from_secs(3)).await;

    cluster.try_commit(1).await?;
    eprintln!("try_commit to nd1 -> ok");

    for i in 0..10 {
        cluster.user(1).fetch_add(i).await?;
    }

    Ok(())
}
