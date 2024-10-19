use anyhow::Result;
use rand::Rng;
use serial_test::serial;
use sorock_tests::*;
use std::sync::Arc;

#[serial]
#[tokio::test(flavor = "multi_thread")]
async fn n1_p10_multi_raft_io() -> Result<()> {
    const P: u32 = 10;
    let cluster = Arc::new(Cluster::new(1, P).await?);

    let mut futs = vec![];
    for shard_id in 0..P {
        let cluster = cluster.clone();
        let fut = async move {
            cluster.add_server(shard_id, 0, 0).await?;
            Ok::<(), anyhow::Error>(())
        };
        futs.push(fut);
    }
    futures::future::try_join_all(futs).await?;

    let mut cur_state = [0; P as usize];
    for _ in 0..100 {
        let shard_id = rand::thread_rng().gen_range(0..P);
        let add_v = rand::thread_rng().gen_range(1..=9);
        let old_v = cluster.user(0).fetch_add(shard_id, add_v).await?;
        assert_eq!(old_v, cur_state[shard_id as usize]);
        cur_state[shard_id as usize] += add_v;
    }

    Ok(())
}
