use anyhow::Result;
use sorock::service::raft::client::*;
use sorock_tests::*;

#[tokio::test(flavor = "multi_thread")]
async fn n1_cluster() -> Result<()> {
    let cluster = Cluster::new(1, 1).await?;
    cluster.add_server(0, 0, 0).await?;

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn n1_write() -> Result<()> {
    let cluster = Cluster::new(1, 1).await?;
    cluster.add_server(0, 0, 0).await?;

    let mut cli = cluster.user(0);
    assert_eq!(cli.fetch_add(0, 1).await?, 0);
    assert_eq!(cli.fetch_add(0, 2).await?, 1);
    assert_eq!(cli.fetch_add(0, 3).await?, 3);

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn n1_read() -> Result<()> {
    let mut cluster = Cluster::new(1, 1).await?;
    cluster.add_server(0, 0, 0).await?;

    let mut cli = cluster.user(0);
    assert_eq!(cli.read(0).await?, 0);
    assert_eq!(cli.fetch_add(0, 1).await?, 0);
    assert_eq!(cli.read(0).await?, 1);
    assert_eq!(cli.fetch_add(0, 2).await?, 1);
    assert_eq!(cli.read(0).await?, 3);

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn n1_snapshot() -> Result<()> {
    let mut cluster = Cluster::new(1, 1).await?;
    cluster.add_server(0, 0, 0).await?;

    for n in 1..10 {
        cluster.user(0).fetch_add(0, n).await?;
    }

    cluster.user(0).make_snapshot(0).await?;

    for n in 1..10 {
        cluster.user(0).fetch_add(0, n).await?;
    }

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn n1_exec_once() -> Result<()> {
    let mut cluster = Cluster::new(1, 1).await?;
    cluster.add_server(0, 0, 0).await?;

    let chan = cluster.env().get_connection(0);
    let cli = RaftClient::new(chan);

    let req = WriteRequest {
        shard_id: 0,
        message: example::AppWriteRequest::FetchAdd {
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

    // Submit the same requests concurrently.
    // But only one of them should be executed.
    futures::future::join_all(futs).await;
    let cur_state = cluster.user(0).read(0).await?;
    assert_eq!(cur_state, 1);

    Ok(())
}
