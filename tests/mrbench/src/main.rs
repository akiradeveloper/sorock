use clap::Parser;
use futures::future::FutureExt;
use sorock_tests::*;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

#[derive(Debug, Parser)]
struct Opts {
    #[clap(long, short = 'n', default_value_t = 1)]
    num_nodes: u8,
    #[clap(long, short = 'p', default_value_t = 1)]
    num_shards: u32,
    #[clap(long="du", short='t', value_parser = humantime::parse_duration, default_value = "1s")]
    io_duration: Duration,
    #[clap(long, short = 'w', default_value_t = 1)]
    n_batch_writes: u32,
    #[clap(long, short = 'r', default_value_t = 1)]
    n_batch_reads: u32,
    #[clap(long, default_value_t = 1)]
    io_size: usize,
    #[clap(long, default_value_t = false)]
    enable_console: bool,
    #[clap(long, default_value_t = false)]
    no_compaction: bool,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let opts = Opts::parse();
    dbg!(&opts);

    if opts.enable_console {
        console_subscriber::init();
    }

    let mut cluster = Cluster::builder()
        .with_logging(false)
        .build(opts.num_nodes, opts.num_shards)
        .await?;

    for node_id in 0..opts.num_nodes {
        let addr = cluster.env().address(node_id);
        eprintln!("node#{node_id} = {addr}");
    }
    let cluster = Arc::new(cluster);

    let t = Instant::now();
    let mut futs = vec![];
    for shard_id in 0..opts.num_shards {
        let cluster = cluster.clone();
        let fut = async move {
            cluster.add_server(shard_id, 0, 0).await?;
            for node_id in 1..opts.num_nodes {
                cluster.add_server(shard_id, 0, node_id).await?;
            }
            Ok::<(), anyhow::Error>(())
        };
        futs.push(fut);
    }
    futures::future::try_join_all(futs).await?;
    eprintln!("cluster setup: {:?}", t.elapsed());

    let t = Instant::now();
    let du = opts.io_duration;
    while t.elapsed() < du {
        let fail_w = Arc::new(AtomicU64::new(0));
        let fail_r = Arc::new(AtomicU64::new(0));
        let mut futs = vec![];
        for shard_id in 0..opts.num_shards {
            for _ in 0..opts.n_batch_writes {
                let error_counter = fail_w.clone();
                let mut cli = cluster.user(0);
                let fut = async move {
                    if let Err(_) = cli.fetch_add(shard_id, opts.io_size as u64).await {
                        error_counter.fetch_add(1, Ordering::Relaxed);
                    }
                }
                .boxed();
                futs.push(fut);
            }
        }
        for shard_id in 0..opts.num_shards {
            for _ in 0..opts.n_batch_reads {
                let error_counter = fail_r.clone();
                let cli = cluster.user(0);
                let fut = async move {
                    if let Err(_) = cli.read(shard_id).await {
                        error_counter.fetch_add(1, Ordering::Relaxed);
                    }
                }
                .boxed();
                futs.push(fut);
            }
        }
        let t = Instant::now();
        futures::future::join_all(futs).await;

        let error_w = fail_w.load(Ordering::Relaxed);
        let error_r = fail_r.load(Ordering::Relaxed);
        eprintln!(
            "io done. {:?}. error=(w={error_w},r={error_r})",
            t.elapsed()
        );

        if !opts.no_compaction {
            let mut futs = vec![];
            for node_id in 0..opts.num_nodes {
                for shard_id in 0..opts.num_shards {
                    // dbg!((node_id, shard_id));
                    let cli = cluster.user(node_id);
                    let fut = async move { cli.make_snapshot(shard_id).await };
                    futs.push(fut);
                }
            }
            if let Err(e) = futures::future::try_join_all(futs).await {
                eprintln!("failed to make snapshot: {:?}", e);
            }
        }
    }

    eprintln!("done");
    Ok(())
}
