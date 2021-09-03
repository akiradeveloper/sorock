use std::sync::atomic::{AtomicU64, Ordering};
static COUNTER: AtomicU64 = AtomicU64::new(0);
fn get_seqid() -> u64 {
    COUNTER.fetch_add(1, Ordering::SeqCst)
}

pub mod cluster;
pub mod env;
pub mod kvs;
pub mod node;
