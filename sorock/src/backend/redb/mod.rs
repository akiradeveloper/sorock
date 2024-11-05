use crate as sorock;

use anyhow::Result;
use async_trait::async_trait;
use crossbeam::channel::TryRecvError;
use redb::{Database, ReadableTable, ReadableTableMetadata, TableDefinition};
use sorock::process::*;
use std::sync::Arc;

mod ballot;
mod log;

pub struct Backend {
    db: Arc<redb::Database>,
    tx: log::Sender,
    _kill_tx: crossbeam::channel::Sender<()>,
}
impl Backend {
    pub fn new(redb: redb::Database) -> Self {
        let db = Arc::new(redb);

        let (reaper, tx) = log::Reaper::new(db.clone());
        let (_kill_tx, kill_rx) = crossbeam::channel::bounded(0);
        std::thread::spawn(move || loop {
            if let Err(TryRecvError::Disconnected) = kill_rx.try_recv() {
                break;
            }
            reaper.reap().ok();
        });

        Self { db, tx, _kill_tx }
    }

    pub fn get(&self, shard_id: u32) -> Result<(impl RaftLogStore, impl RaftBallotStore)> {
        let log = log::LogStore::new(self.db.clone(), shard_id, self.tx.clone())?;
        let ballot = ballot::BallotStore::new(self.db.clone(), shard_id)?;
        Ok((log, ballot))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn basic_test() -> Result<()> {
        let mem = redb::backends::InMemoryBackend::new();
        let db = redb::Database::builder().create_with_backend(mem)?;
        let db = Backend::new(db);

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

        let (log, _) = db.get(0)?;

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

        let mem = redb::backends::InMemoryBackend::new();
        let db = redb::Database::builder().create_with_backend(mem)?;
        let db = Arc::new(Backend::new(db));

        let mut futs = vec![];
        for shard in 0..100 {
            let db = db.clone();
            let fut = async move {
                let mut rng = rand::thread_rng();
                let (log, _) = db.get(shard).unwrap();
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
                let (log, _) = db.get(shard_id).unwrap();
                let e = log.get_entry(i).await.unwrap().unwrap();
                assert_eq!(e.this_clock.index, i);
            }
        }

        Ok(())
    }
}
