use anyhow::Result;
use lol_tests::*;
use rand::Rng;
use serial_test::serial;

#[serial]
#[tokio::test(flavor = "multi_thread")]
async fn n3_cluster() -> Result<()> {
    let mut cluster = Cluster::new(3, 1).await?;
    cluster.add_server(0, 0, 0).await?;
    cluster.add_server(0, 0, 1).await?;
    cluster.add_server(0, 1, 2).await?;
    Ok(())
}

#[serial]
#[tokio::test(flavor = "multi_thread")]
async fn n3_write() -> Result<()> {
    let mut cluster = Cluster::new(3, 1).await?;
    cluster.add_server(0, 0, 0).await?;
    cluster.add_server(0, 0, 1).await?;
    cluster.add_server(0, 1, 2).await?;

    let mut cur_state = 0;
    for i in 0..100 {
        let add_v = rand::thread_rng().gen_range(1..=9);
        let io_node = (i % 3) as u8;
        let old_v = cluster.user(io_node).fetch_add(0, add_v).await?;
        assert_eq!(old_v, cur_state);
        cur_state += add_v;
    }

    let expected = cluster.user(0).read(0).await?;
    assert_eq!(expected, cur_state);

    Ok(())
}

#[serial]
#[tokio::test(flavor = "multi_thread")]
async fn n3_par_write() -> Result<()> {
    const N: u64 = 100;

    let mut cluster = Cluster::new(3, 1).await?;
    cluster.add_server(0, 0, 0).await?;
    cluster.add_server(0, 0, 1).await?;
    cluster.add_server(0, 1, 2).await?;

    let mut futs = vec![];
    for _ in 0..N {
        let mut cli = cluster.user(0);
        let fut = async move { cli.fetch_add(0, 1).await };
        futs.push(fut);
    }
    futures::future::try_join_all(futs).await?;

    let expected = cluster.user(1).read(0).await?;
    assert_eq!(expected, N);

    Ok(())
}

#[serial]
#[tokio::test(flavor = "multi_thread")]
async fn n3_snapshot() -> Result<()> {
    let mut cluster = Cluster::new(3, 1).await?;
    cluster.add_server(0, 0, 0).await?;

    cluster.user(0).fetch_add(0, 1).await?;
    cluster.user(0).fetch_add(0, 10).await?;
    cluster.user(0).fetch_add(0, 100).await?;
    cluster.user(0).make_snapshot(0).await?;

    cluster.add_server(0, 0, 1).await?;
    cluster.add_server(0, 0, 2).await?;

    assert_eq!(cluster.user(0).fetch_add(0, 1000).await?, 111);
    assert_eq!(cluster.user(0).fetch_add(0, 10000).await?, 1111);

    Ok(())
}

#[serial]
#[tokio::test(flavor = "multi_thread")]
async fn n3_leader_stop() -> Result<()> {
    let mut cluster = Cluster::new(3, 1).await?;
    cluster.add_server(0, 0, 0).await?;

    for i in 0..10 {
        cluster.user(0).fetch_add(0, i).await?;
    }

    cluster.add_server(0, 0, 1).await?;
    cluster.add_server(0, 0, 2).await?;

    // Down the node
    // The actual intention here is stop the node, not remove it.
    // Since the tokio runtime doesn't support disabling the scheduler,
    // we have no way but to remove the node.
    cluster.env().remove_node(0);
    tokio::time::sleep(tokio::time::Duration::from_secs(3)).await;

    for i in 0..10 {
        cluster.user(1).fetch_add(0, i).await?;
    }

    Ok(())
}

#[serial]
#[tokio::test(flavor = "multi_thread")]
async fn n3_leader_stepdown() -> Result<()> {
    let mut cluster = Cluster::new(3, 1).await?;
    cluster.add_server(0, 0, 0).await?;

    for i in 0..10 {
        cluster.user(0).fetch_add(0, i).await?;
    }

    cluster.add_server(0, 0, 1).await?;
    cluster.add_server(0, 0, 2).await?;

    cluster.remove_server(0, 1, 0).await?;
    eprintln!("removed nd0 -> ok");
    tokio::time::sleep(tokio::time::Duration::from_secs(3)).await;

    for i in 0..10 {
        cluster.user(1).fetch_add(0, i).await?;
    }

    Ok(())
}

#[serial]
#[tokio::test(flavor = "multi_thread")]
async fn n3_down2_err() -> Result<()> {
    let mut cluster = Cluster::new(3, 1).await?;
    cluster.add_server(0, 0, 0).await?;
    cluster.add_server(0, 0, 1).await?;
    cluster.add_server(0, 0, 2).await?;

    cluster.user(0).fetch_add(0, 1).await?;

    cluster.env().remove_node(1); // down
    cluster.user(0).fetch_add(0, 2).await?;

    cluster.env().remove_node(2); // down
    assert!(cluster.user(0).fetch_add(0, 4).await.is_err());

    Ok(())
}
