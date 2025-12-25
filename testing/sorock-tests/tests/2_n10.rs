use anyhow::Result;
use rand::Rng;
use sorock_tests::*;

#[tokio::test(flavor = "multi_thread")]
async fn n10_cluster() -> Result<()> {
    let cluster = Cluster::new(10, 1).await?;
    cluster.add_voter(0, 0, 0).await?;
    for i in 0..9 {
        cluster.add_voter(0, i, i + 1).await?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn n10_write() -> Result<()> {
    let cluster = Cluster::new(10, 1).await?;
    cluster.add_voter(0, 0, 0).await?;
    for i in 0..9 {
        cluster.add_voter(0, i, i + 1).await?;
    }

    let mut cur_state = 0;
    for i in 0..100 {
        let add_v = rand::rng().random_range(1..=9);
        let io_node = (i % 10) as u8;
        let old_v = cluster.user(io_node).fetch_add(0, add_v).await?;
        assert_eq!(old_v, cur_state);
        cur_state += add_v;
    }

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn n10_read() -> Result<()> {
    let cluster = Cluster::new(10, 1).await?;
    cluster.add_voter(0, 0, 0).await?;
    for i in 0..9 {
        cluster.add_voter(0, i, i + 1).await?;
    }

    let mut cur_state = 0;
    for _ in 0..1000 {
        let add_v = rand::rng().random_range(1..=9);
        cluster.user(0).fetch_add(0, add_v).await?;
        cur_state += add_v;
    }

    for id in (1..10).rev() {
        let expected = cluster.user(id).read(0).await?;
        assert_eq!(expected, cur_state);
    }

    Ok(())
}
