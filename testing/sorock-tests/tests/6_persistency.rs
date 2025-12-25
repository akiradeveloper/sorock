use anyhow::Result;
use rand::Rng;
use sorock_tests::*;
use std::time::Duration;

#[tokio::test(flavor = "multi_thread")]
async fn n3_restore() -> Result<()> {
    let mut cluster = Cluster::builder()
        .with_persistency(true)
        .build(3, 1)
        .await?;
    cluster.add_voter(0, 0, 0).await?;
    cluster.add_voter(0, 0, 1).await?;
    cluster.add_voter(0, 1, 2).await?;

    let mut cur_state = 0;
    for i in 0..10 {
        let add_v = rand::rng().random_range(1..=9);
        let old_v = cluster.user(0).fetch_add(0, add_v).await?;
        assert_eq!(old_v, cur_state);
        cur_state += add_v;

        if i == 5 {
            cluster.user(0).make_snapshot(0).await?;
            cluster.user(1).make_snapshot(0).await?;
        }
    }

    cluster.env_mut().remove_node(0);
    cluster.env_mut().remove_node(1);
    cluster.env_mut().remove_node(2);
    tokio::time::sleep(Duration::from_secs(1)).await;

    cluster.env_mut().add_node(0);
    cluster.env().check_connectivity(0).await?;
    cluster.env_mut().add_node(1);
    cluster.env().check_connectivity(1).await?;
    // Wait for election.
    tokio::time::sleep(Duration::from_secs(5)).await;
    assert_eq!(cluster.user(1).read(0).await?, cur_state);

    Ok(())
}
