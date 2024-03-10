use anyhow::Result;
use lol_tests::*;
use serial_test::serial;
use test_log::test;

#[serial]
#[test(tokio::test(flavor = "multi_thread"))]
async fn n1_cluster() -> Result<()> {
    let mut cluster = Cluster::new(1).await?;
    cluster.add_server(0, 0).await?;

    Ok(())
}

#[serial]
#[test(tokio::test(flavor = "multi_thread"))]
async fn n1_write() -> Result<()> {
    let mut cluster = Cluster::new(1).await?;
    cluster.add_server(0, 0).await?;

    let mut cli = cluster.user(0);
    assert_eq!(cli.fetch_add(1).await?, 0);
    assert_eq!(cli.fetch_add(2).await?, 1);
    assert_eq!(cli.fetch_add(3).await?, 3);

    Ok(())
}

#[serial]
#[test(tokio::test(flavor = "multi_thread"))]
async fn n1_read() -> Result<()> {
    let mut cluster = Cluster::new(1).await?;
    cluster.add_server(0, 0).await?;

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
async fn n1_snapshot() -> Result<()> {
    let mut cluster = Cluster::new(1).await?;
    cluster.add_server(0, 0).await?;

    for n in 1..10 {
        cluster.user(0).fetch_add(n).await?;
    }

    cluster.user(0).make_snapshot().await?;

    for n in 1..10 {
        cluster.user(0).fetch_add(n).await?;
    }

    Ok(())
}

#[serial]
#[test(tokio::test(flavor = "multi_thread"))]
async fn n1_many_retry_exec_once() -> Result<()> {
    let mut cluster = Cluster::new(1).await?;
    cluster.add_server(0, 0).await?;

    let chan = cluster.env().connect(0);
    let cli = lol2::client::RaftClient::new(chan);

    let req = lol2::client::WriteRequest {
        lane_id: testapp::APP_LANE_ID,
        message: testapp::AppWriteRequest::FetchAdd {
            bytes: vec![1u8; 1].into(),
        }
        .serialize(),
        request_id: uuid::Uuid::new_v4().to_string(),
    };

    let mut futs = vec![];
    for _ in 0..100 {
        let mut cli = cli.clone();
        let req = req.clone();
        let fut = async move { cli.write(req).await };
        futs.push(fut);
    }
    futures::future::join_all(futs).await;

    let cur_state = cluster.user(0).read().await?;
    // executed only once.
    assert_eq!(cur_state, 1);

    Ok(())
}
