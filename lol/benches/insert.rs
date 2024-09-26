#![feature(test)]

use lol::process::{Clock, Entry, RaftLogStore};

extern crate test;

pub fn randbytes(n: usize) -> Vec<u8> {
    use rand::Rng;
    let mut rng = rand::thread_rng();
    let mut out = vec![0; n];
    rng.fill(&mut out[..]);
    out
}

/// n bytes
fn do_bench(n: usize, b: &mut test::Bencher) {
    let mut rt = tokio::runtime::Runtime::new().unwrap();

    let mem = redb::backends::InMemoryBackend::new();
    let db = redb::Database::builder().create_with_backend(mem).unwrap();
    let db = lol::backend::Backend::new(db);
    let (log, _) = db.get(0).unwrap();

    let mut i = 0;
    b.iter(|| {
        i += 1;
        let bytes = randbytes(n);
        let entry = Entry {
            prev_clock: Clock {
                index: i - 1,
                term: 1,
            },
            this_clock: Clock { index: i, term: 1 },
            command: bytes.into(),
        };
        let fut = log.insert_entry(i, entry);
        rt.block_on(fut).unwrap();
    });
}

#[bench]
fn insert_1(b: &mut test::Bencher) {
    do_bench(1, b);
}
#[bench]
fn insert_100(b: &mut test::Bencher) {
    do_bench(100, b);
}
#[bench]
fn insert_10k(b: &mut test::Bencher) {
    do_bench(10000, b);
}
#[bench]
fn insert_1m(b: &mut test::Bencher) {
    do_bench(1000000, b);
}
