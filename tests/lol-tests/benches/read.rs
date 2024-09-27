#![feature(test)]

use lol_tests::*;

extern crate test;

/// m parallel
fn do_bench(m: u16, b: &mut test::Bencher) {
    let rt = tokio::runtime::Runtime::new().unwrap();

    let cluster = rt.block_on(async {
        let cluster = Cluster::builder()
            .with_logging(false)
            .build(1, 1)
            .await
            .unwrap();
        cluster.add_server(0, 0, 0).await.unwrap();
        cluster
    });

    let mut cli = cluster.user(0);
    rt.block_on(async move {
        cli.fetch_add(0, 1).await.unwrap();
    });

    b.iter(|| {
        let mut futs = vec![];
        for _ in 0..m {
            let cli = cluster.user(0);
            let fut = async move { cli.read(0).await };
            futs.push(fut);
        }
        rt.block_on(futures::future::try_join_all(futs)).unwrap();
    });
}

#[bench]
fn read_n1_par1(b: &mut test::Bencher) {
    do_bench(1, b);
}
#[bench]
fn read_n1_par10(b: &mut test::Bencher) {
    do_bench(10, b);
}
#[bench]
fn read_n1_par100(b: &mut test::Bencher) {
    do_bench(100, b);
}
#[bench]
fn read_n1_par1000(b: &mut test::Bencher) {
    do_bench(1000, b);
}
