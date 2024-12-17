use crate as sorock;

mod ballot;
mod log;

use std::{collections::BTreeMap, sync::Arc};

use anyhow::Result;
use async_trait::async_trait;
use ballot::BallotStore;
use crossbeam::channel::{Receiver, Sender, TryRecvError};
use log::LogStore;
use sorock::process::*;
use spin::Mutex;

#[derive(Clone)]
pub struct Backend {
    log_store_map: Arc<Mutex<BTreeMap<String, LogStore>>>,
    _ballot_store_map: Arc<BTreeMap<String, BallotStore>>,
    tx: log::Sender,
    _kill_tx: crossbeam::channel::Sender<()>,
}

impl Backend {
    pub fn new() -> Self {
        let log_store_map = Arc::new(Mutex::new(BTreeMap::new()));
        let _ballot_store_map = Arc::new(BTreeMap::new());
        let (reaper, tx) = log::Reaper::new(log_store_map.clone());
        let (_kill_tx, kill_rx): (Sender<()>, Receiver<()>) = crossbeam::channel::bounded(0);

        std::thread::spawn(move || loop {
            if let Err(TryRecvError::Disconnected) = kill_rx.try_recv() {
                break;
            }
            reaper.reap().ok();
        });

        Self {
            log_store_map,
            _ballot_store_map,
            tx,
            _kill_tx,
        }
    }

    pub fn get(&self, index: u32) -> Result<(impl RaftLogStore, impl RaftBallotStore)> {
        let log_space = format!("log-{index}");

        let mut log = self.log_store_map.lock();
        let log_store = log
            .entry(log_space.clone())
            .or_insert_with(|| LogStore::new(index, self.tx.clone()).unwrap())
            .clone();

        let ballot_store = ballot::BallotStore::new();

        Ok((log_store, ballot_store))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn basic_test() -> Result<()> {
        let b_tree_map = Backend::new();

        let entry1 = Entry {
            prev_clock: Clock { index: 0, term: 0 },
            this_clock: Clock { index: 1, term: 1 },
            command: bytes::Bytes::from("hello"),
        };
        let entry2 = Entry {
            prev_clock: Clock { index: 1, term: 1 },
            this_clock: Clock { index: 2, term: 1 },
            command: bytes::Bytes::from("world"),
        };

        let (log, _) = b_tree_map.get(0)?;

        assert!(log.get_entry(1).await?.is_none());
        assert!(log.get_entry(2).await?.is_none());

        log.insert_entry(1, entry1).await?;
        assert_eq!(log.get_head_index().await?, 1);
        assert_eq!(log.get_last_index().await?, 1);
        assert!(log.get_entry(1).await?.is_some());
        assert!(log.get_entry(2).await?.is_none());

        log.insert_entry(2, entry2).await?;
        assert_eq!(log.get_head_index().await?, 1);
        assert_eq!(log.get_last_index().await?, 2);
        assert!(log.get_entry(1).await?.is_some());
        assert!(log.get_entry(2).await?.is_some());

        log.delete_entries_before(2).await?;
        assert_eq!(log.get_head_index().await?, 2);
        assert_eq!(log.get_last_index().await?, 2);
        assert!(log.get_entry(1).await?.is_none());
        assert!(log.get_entry(2).await?.is_some());

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn insert_test() -> Result<()> {
        use rand::Rng;

        let b_tree_map = Arc::new(Backend::new());

        let mut futs = vec![];
        for shard in 0..100 {
            let b_tree_map = b_tree_map.clone();
            let fut = async move {
                let mut rng = rand::thread_rng();
                let (log, _) = b_tree_map.get(shard).unwrap();
                for i in 0..300 {
                    let prev = i;
                    let cur = i + 1;
                    let b: Vec<u8> = (0..100).map(|_| rng.gen()).collect();
                    let e = Entry {
                        prev_clock: Clock {
                            index: prev,
                            term: 1,
                        },
                        this_clock: Clock {
                            index: cur,
                            term: 1,
                        },
                        command: b.into(),
                    };
                    log.insert_entry(cur, e).await.unwrap();
                }
            };
            futs.push(fut);
        }

        futures::future::join_all(futs).await;

        for shard_id in 0..100 {
            for i in 1..=100 {
                let (log, _) = b_tree_map.get(shard_id).unwrap();
                let e = log.get_entry(i).await.unwrap().unwrap();
                assert_eq!(e.this_clock.index, i);
            }
        }

        Ok(())
    }
}
