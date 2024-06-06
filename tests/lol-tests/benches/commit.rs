#![feature(test)]

use lol_tests::*;

extern crate test;

/// n nodes, m parallel
fn do_bench(n: u8, m: u16, b: &mut test::Bencher) {
    let mut rt = tokio::runtime::Runtime::new().unwrap();

    let cluster = rt.block_on(async {
        let mut cluster = Cluster::new(n, 1).await.unwrap();
        for i in 0..n {
            cluster.add_server(0, 0, i).await.unwrap();
        }
        cluster
    });

    b.iter(|| {
        let mut futs = vec![];
        for _ in 0..m {
            let mut cli = cluster.user(0);
            let fut = async move { cli.fetch_add(0, 1).await };
            futs.push(fut);
        }
        rt.block_on(futures::future::try_join_all(futs)).unwrap();
    });

    rt.block_on(async {
        drop(cluster);
    });
}

#[bench]
fn commit_n1_m1(b: &mut test::Bencher) {
    do_bench(1, 1, b);
}
#[bench]
fn commit_n1_m10(b: &mut test::Bencher) {
    do_bench(1, 10, b);
}
#[bench]
fn commit_n1_m100(b: &mut test::Bencher) {
    do_bench(1, 100, b);
}
#[bench]
fn commit_n1_m1000(b: &mut test::Bencher) {
    do_bench(1, 1000, b);
}
#[bench]
fn commit_n3_m1(b: &mut test::Bencher) {
    do_bench(3, 1, b);
}
#[bench]
fn commit_n3_m10(b: &mut test::Bencher) {
    do_bench(3, 10, b);
}