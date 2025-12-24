use anyhow::Result;
use sorock_tests::*;

#[tokio::test(flavor = "multi_thread")]
async fn n1_learner_cant_process() -> Result<()> {
    let cluster = Cluster::new(1, 1).await?;
    cluster.add_learner(0, 0, 0).await?;

    let add_res = cluster.user(0).fetch_add(0, 1).await;
    assert!(add_res.is_err());

    let read_res = cluster.user(0).read(0).await;
    assert!(read_res.is_err());

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn n3_two_learners_can_read() -> Result<()> {
    let cluster = Cluster::new(3, 1).await?;
    cluster.add_voter(0, 0, 0).await?;
    cluster.add_learner(0, 0, 1).await?;
    cluster.add_learner(0, 0, 2).await?;
    std::thread::sleep(std::time::Duration::from_secs(1));

    let add_res = cluster.user(0).fetch_add(0, 1).await;
    assert!(add_res.is_ok());

    let read_res = cluster.user(1).read(0).await;
    assert!(read_res.is_ok());

    let read_res = cluster.user(2).read(0).await;
    assert!(read_res.is_ok());

    Ok(())
}
