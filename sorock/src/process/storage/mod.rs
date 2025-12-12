use crate as sorock;

use anyhow::Result;
use crossbeam::channel::TryRecvError;
use redb::{Database, ReadableTable, ReadableTableMetadata, TableDefinition};
use serde::{Deserialize, Serialize};
use sorock::process::*;
use std::sync::Arc;
use log_storage::{LogStorage, LogShardView};

mod ballot;
mod log;

pub use ballot::BallotStore;
pub use log::LogStore;

/// `RaftStorage` is a storage backend for `RaftProcess` based on redb.
pub struct RaftStorage {
    db: Arc<redb::Database>,
    log_storage: LogStorage,
}
impl RaftStorage {
    pub fn new(redb: redb::Database) -> Self {
        let db = Arc::new(redb);
        let log_storage = LogStorage::new(db.clone());
        Self { db, log_storage }
    }

    pub(super) fn get(
        &self,
        shard_index: ShardIndex,
    ) -> Result<(log::LogStore, ballot::BallotStore)> {
        let view = self.log_storage.get_shard(shard_index)?;
        let log = log::LogStore::new(view);

        let ballot = ballot::BallotStore::new(self.db.clone(), shard_index)?;

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
        let db = RaftStorage::new(db);

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
        let db = Arc::new(RaftStorage::new(db));

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

        for shard_index in 0..100 {
            for i in 1..=100 {
                let (log, _) = db.get(shard_index).unwrap();
                let e = log.get_entry(i).await.unwrap().unwrap();
                assert_eq!(e.this_clock.index, i);
            }
        }

        Ok(())
    }
}
