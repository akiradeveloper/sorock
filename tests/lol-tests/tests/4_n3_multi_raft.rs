use anyhow::Result;
use lol_tests::*;
use lolraft::client::TimeoutNow;
use rand::Rng;
use serial_test::serial;
use std::sync::Arc;

#[serial]
#[tokio::test(flavor = "multi_thread")]
async fn n3_p10_multi_raft_cluster() -> Result<()> {
    const P: u32 = 10;
    let cluster = Arc::new(Cluster::new(3, P).await?);

    let mut futs = vec![];
    for shard_id in 0..P {
        let cluster = cluster.clone();
        let fut = async move {
            cluster.add_server(shard_id, 0, 0).await?;
            cluster.add_server(shard_id, 0, 1).await?;
            cluster.add_server(shard_id, 0, 2).await?;

            // Evenly distribute the leaders.
            let leader = (shard_id % 3) as u8;
            cluster
                .admin(leader)
                .send_timeout_now(TimeoutNow { shard_id })
                .await?;

            Ok::<(), anyhow::Error>(())
        };
        futs.push(fut);
    }
    futures::future::try_join_all(futs).await?;

    Ok(())
}

#[serial]
#[tokio::test(flavor = "multi_thread")]
async fn n3_p10_multi_raft_io() -> Result<()> {
    const L: u32 = 10;

    let cluster = Arc::new(Cluster::new(3, L).await?);

    let mut futs = vec![];
    for shard_id in 0..L {
        let cluster = cluster.clone();
        let fut = async move {
            cluster.add_server(shard_id, 0, 0).await?;
            cluster.add_server(shard_id, 0, 1).await?;
            cluster.add_server(shard_id, 0, 2).await?;
            Ok::<(), anyhow::Error>(())
        };
        futs.push(fut);
    }
    futures::future::try_join_all(futs).await?;

    let mut cur_state = [0; L as usize];
    for _ in 0..100 {
        let shard_id = rand::thread_rng().gen_range(0..L);
        let add_v = rand::thread_rng().gen_range(1..=9);
        let old_v = cluster.user(0).fetch_add(shard_id, add_v).await?;
        assert_eq!(old_v, cur_state[shard_id as usize]);
        cur_state[shard_id as usize] += add_v;
    }

    Ok(())
}

#[serial]
#[tokio::test(flavor = "multi_thread")]
async fn n3_p10_multi_raft_io_roundrobin() -> Result<()> {
    const P: u32 = 10;

    let cluster = Arc::new(Cluster::new(3, P).await?);

    let mut futs = vec![];
    for shard_id in 0..P {
        let cluster = cluster.clone();
        let fut = async move {
            cluster.add_server(shard_id, 0, 0).await?;
            cluster.add_server(shard_id, 0, 1).await?;
            cluster.add_server(shard_id, 0, 2).await?;
            Ok::<(), anyhow::Error>(())
        };
        futs.push(fut);
    }
    futures::future::try_join_all(futs).await?;

    let mut cur_state = [0; P as usize];
    for i in 0..100 {
        let shard_id = rand::thread_rng().gen_range(0..P);
        let add_v = rand::thread_rng().gen_range(1..=9);
        let io_node = (i % 3) as u8;
        let old_v = cluster.user(io_node).fetch_add(shard_id, add_v).await?;
        assert_eq!(old_v, cur_state[shard_id as usize]);
        cur_state[shard_id as usize] += add_v;
    }

    Ok(())
}
