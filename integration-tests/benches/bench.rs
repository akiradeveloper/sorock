#![feature(test)]

use integration_tests::cluster::*;
use integration_tests::env::NodeCommand;
use integration_tests::kvs::*;

use bytes::Bytes;
use lol_core::RaftClient;
use lol_core::api;
use std::thread;
use std::time::Duration;
use tonic::transport::channel::Endpoint;

extern crate test;

fn do_bench_commit(n: u8, b: &mut test::Bencher) {
    let mut rt = tokio::runtime::Runtime::new().unwrap();
    let env = init_cluster(n);
    let id = env.get_node_id(0);

    let v = Bytes::from("v");
    b.iter(|| {
        let endpoint = Endpoint::from_shared(id.clone()).unwrap();
        let msg = kvs::Req::SetBytes {
            key: "k".to_owned(),
            value: v.clone(),
        };
        let msg = kvs::Req::serialize(&msg);
        let r: anyhow::Result<_> = rt.block_on(async move {
            let mut conn = RaftClient::connect(endpoint).await?;
            let res = conn
                .request_commit(api::CommitReq {
                    message: msg,
                })
                .await?;
            Ok(res)
        });
        assert!(r.is_ok());
    })
}
#[bench]
fn bench_commit_1(b: &mut test::Bencher) {
    do_bench_commit(1, b)
}
#[bench]
fn bench_commit_4(b: &mut test::Bencher) {
    do_bench_commit(4, b)
}
#[bench]
fn bench_commit_16(b: &mut test::Bencher) {
    do_bench_commit(16, b)
}
#[bench]
fn bench_commit_64(b: &mut test::Bencher) {
    do_bench_commit(64, b)
}

fn do_bench_apply(n: u8, b: &mut test::Bencher) {
    let mut rt = tokio::runtime::Runtime::new().unwrap();
    let env = init_cluster(n);
    let id = env.get_node_id(0);

    let v = Bytes::from("v");
    b.iter(|| {
        let endpoint = Endpoint::from_shared(id.clone()).unwrap();
        let msg = kvs::Req::SetBytes {
            key: "k".to_owned(),
            value: v.clone(),
        };
        let msg = kvs::Req::serialize(&msg);
        let r: anyhow::Result<_> = rt.block_on(async move {
            let mut conn = RaftClient::connect(endpoint).await?;
            let res = conn
                .request_apply(api::ApplyReq {
                    mutation: true,
                    message: msg,
                })
                .await?;
            Ok(res)
        });
        assert!(r.is_ok());
    })
}
#[bench]
fn bench_apply_1(b: &mut test::Bencher) {
    do_bench_apply(1, b)
}
#[bench]
fn bench_apply_4(b: &mut test::Bencher) {
    do_bench_apply(4, b)
}
#[bench]
fn bench_apply_16(b: &mut test::Bencher) {
    do_bench_apply(16, b)
}
#[bench]
fn bench_apply_64(b: &mut test::Bencher) {
    do_bench_apply(64, b)
}

fn do_bench_query(n: u8, b: &mut test::Bencher) {
    let mut rt = tokio::runtime::Runtime::new().unwrap();
    let env = init_cluster(n);
    Client::to(0, env.clone()).set("k", "v");
    thread::sleep(Duration::from_secs(1));

    let id = env.get_node_id(0);
    b.iter(|| {
        let endpoint = Endpoint::from_shared(id.clone()).unwrap();
        let msg = kvs::Req::Get {
            key: "k".to_owned(),
        };
        let msg = kvs::Req::serialize(&msg);
        let r: anyhow::Result<_> = rt.block_on(async move {
            let mut conn = RaftClient::connect(endpoint).await?;
            let res = conn
                .request_apply(api::ApplyReq {
                    mutation: false,
                    message: msg,
                })
                .await?;
            Ok(res)
        });
        assert!(r.is_ok());
    })
}
#[bench]
fn bench_query_1(b: &mut test::Bencher) {
    do_bench_query(1, b);
}
#[bench]
fn bench_query_4(b: &mut test::Bencher) {
    do_bench_query(4, b);
}
#[bench]
fn bench_query_16(b: &mut test::Bencher) {
    do_bench_query(16, b);
}
#[bench]
fn bench_query_64(b: &mut test::Bencher) {
    do_bench_query(64, b);
}

fn do_bench_commit_huge(n: u8, command: impl Fn(u8) -> NodeCommand, b: &mut test::Bencher) {
    let mut rt = tokio::runtime::Runtime::new().unwrap();
    let env = make_cluster(n, command);
    let id = env.get_node_id(0);

    let v = Bytes::from(vec![1; 1_000_000]);
    b.iter(|| {
        let endpoint = Endpoint::from_shared(id.clone()).unwrap();
        let msg = kvs::Req::SetBytes {
            key: "k".to_owned(),
            value: v.clone(),
        };
        let msg = kvs::Req::serialize(&msg);
        let r: anyhow::Result<_> = rt.block_on(async move {
            let mut conn = RaftClient::connect(endpoint).await?;
            let res = conn
                .request_commit(api::CommitReq {
                    message: msg,
                })
                .await?;
            Ok(res)
        });
        assert!(r.is_ok());
    })
}
fn command_mem(i: u8) -> NodeCommand {
    NodeCommand::new("kvs-server")
}
fn command_rocks(i: u8) -> NodeCommand {
    let s = format!("--use-persistency={}", i);
    NodeCommand::new("kvs-server").with_args(vec![s.as_str(), "--reset-persistency"])
}
#[bench]
fn bench_commit_huge_1_mem(b: &mut test::Bencher) {
    do_bench_commit_huge(1, command_mem, b)
}
#[bench]
fn bench_commit_huge_4_mem(b: &mut test::Bencher) {
    do_bench_commit_huge(4, command_mem, b)
}
#[bench]
fn bench_commit_huge_16_mem(b: &mut test::Bencher) {
    do_bench_commit_huge(16, command_mem, b)
}
#[bench]
fn bench_commit_huge_64_mem(b: &mut test::Bencher) {
    do_bench_commit_huge(64, command_mem, b)
}
#[bench]
fn bench_commit_huge_1_rocks(b: &mut test::Bencher) {
    do_bench_commit_huge(1, command_rocks, b)
}
#[bench]
fn bench_commit_huge_4_rocks(b: &mut test::Bencher) {
    do_bench_commit_huge(4, command_rocks, b)
}
#[bench]
fn bench_commit_huge_16_rocks(b: &mut test::Bencher) {
    do_bench_commit_huge(16, command_rocks, b)
}
#[bench]
fn bench_commit_huge_64_rocks(b: &mut test::Bencher) {
    do_bench_commit_huge(64, command_rocks, b)
}
