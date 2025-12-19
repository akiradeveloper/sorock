use anyhow::Result;
use rand::Rng;
use sorock::service::raft::client::*;
use sorock_tests::*;
use std::sync::Arc;

#[tokio::test(flavor = "multi_thread")]
async fn n3_p10_multi_raft_cluster() -> Result<()> {
    const N: u8 = 3;
    const P: u32 = 10;
    let cluster = Arc::new(Cluster::new(N, P).await?);

    let mut futs = vec![];
    for shard_index in 0..P {
        let cluster = cluster.clone();
        let fut = async move {
            for node_id in 0..N {
                cluster.add_server(shard_index, 0, node_id).await?;
            }

            // Evenly distribute the leaders.
            let leader = (shard_index % 3) as u8;
            cluster
                .admin(leader)
                .send_timeout_now(TimeoutNow { shard_index })
                .await?;

            Ok::<(), anyhow::Error>(())
        };
        futs.push(fut);
    }
    futures::future::try_join_all(futs).await?;

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn n3_p10_multi_raft_io() -> Result<()> {
    const N: u8 = 3;
    const P: u32 = 10;

    let cluster = Arc::new(Cluster::new(N, P).await?);

    let mut futs = vec![];
    for shard_index in 0..P {
        let cluster = cluster.clone();
        let fut = async move {
            for node_id in 0..N {
                cluster.add_server(shard_index, 0, node_id).await?;
            }
            Ok::<(), anyhow::Error>(())
        };
        futs.push(fut);
    }
    futures::future::try_join_all(futs).await?;

    let mut cur_state = [0; P as usize];
    for _ in 0..100 {
        let shard_index = rand::thread_rng().gen_range(0..P);
        let add_v = rand::thread_rng().gen_range(1..=9);
        let old_v = cluster.user(0).fetch_add(shard_index, add_v).await?;
        assert_eq!(old_v, cur_state[shard_index as usize]);
        cur_state[shard_index as usize] += add_v;
    }

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn n3_p10_multi_raft_io_roundrobin() -> Result<()> {
    const N: u8 = 3;
    const P: u32 = 10;

    let cluster = Arc::new(Cluster::new(3, P).await?);

    let mut futs = vec![];
    for shard_index in 0..P {
        let cluster = cluster.clone();
        let fut = async move {
            for node_id in 0..N {
                cluster.add_server(shard_index, 0, node_id).await?;
            }
            Ok::<(), anyhow::Error>(())
        };
        futs.push(fut);
    }
    futures::future::try_join_all(futs).await?;

    let mut cur_state = [0; P as usize];
    for i in 0..100 {
        let shard_index = rand::thread_rng().gen_range(0..P);
        let add_v = rand::thread_rng().gen_range(1..=9);
        let io_node = (i % 3) as u8;
        let old_v = cluster.user(io_node).fetch_add(shard_index, add_v).await?;
        assert_eq!(old_v, cur_state[shard_index as usize]);
        cur_state[shard_index as usize] += add_v;
    }

    Ok(())
}
