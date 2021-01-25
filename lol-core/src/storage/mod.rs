use crate::{Clock, Id, Index, Term};
use bytes::Bytes;
use std::collections::BTreeSet;

/// In-memory implementation backed by BTreeMap.
pub mod memory;

/// Persistent implementation backed by RocksDB.
#[cfg(feature = "persistency")]
#[cfg_attr(docsrs, doc(cfg(feature = "persistency")))]
pub mod disk;

#[derive(Clone, Debug, PartialEq)]
pub struct Ballot {
    pub(crate) cur_term: Term,
    pub(crate) voted_for: Option<Id>,
}
impl Ballot {
    fn new() -> Self {
        Self {
            cur_term: 0,
            voted_for: None,
        }
    }
}

#[derive(Clone)]
pub struct Entry {
    pub(crate) prev_clock: Clock,
    pub(crate) this_clock: Clock,
    pub(crate) command: Bytes,
}

/// The abstraction for the backing storage.
/// Conceptually it is considered as a sequence of log entries and the recent vote.
#[async_trait::async_trait]
pub trait RaftStorage: Sync + Send + 'static {
    /// Delete range ..r
    async fn delete_before(&self, r: Index) -> anyhow::Result<()>;
    /// Save the snapshot entry so snapshot index always advance.
    async fn insert_snapshot(&self, i: Index, e: Entry) -> anyhow::Result<()>;
    async fn insert_entry(&self, i: Index, e: Entry) -> anyhow::Result<()>;
    async fn get_entry(&self, i: Index) -> anyhow::Result<Option<Entry>>;
    async fn get_snapshot_index(&self) -> anyhow::Result<Index>;
    async fn get_last_index(&self) -> anyhow::Result<Index>;
    async fn save_ballot(&self, v: Ballot) -> anyhow::Result<()>;
    async fn load_ballot(&self) -> anyhow::Result<Ballot>;
    async fn put_tag(&self, i: Index, snapshot: crate::SnapshotTag) -> anyhow::Result<()>;
    async fn delete_tag(&self, i: Index) -> anyhow::Result<()>;
    async fn get_tag(&self, i: Index) -> anyhow::Result<Option<crate::SnapshotTag>>;
    async fn list_tags(&self) -> anyhow::Result<BTreeSet<Index>>;
}

#[cfg(test)]
async fn test_storage<S: RaftStorage>(s: S) -> anyhow::Result<()> {
    let e = Entry {
        prev_clock: Clock { term: 0, index: 0 },
        this_clock: Clock { term: 0, index: 0 },
        command: Bytes::new(),
    };

    // Vote
    let id = "hoge".to_owned();
    assert_eq!(
        s.load_ballot().await?,
        Ballot {
            cur_term: 0,
            voted_for: None
        }
    );
    s.save_ballot(Ballot {
        cur_term: 1,
        voted_for: Some(id.clone()),
    })
    .await?;
    assert_eq!(
        s.load_ballot().await?,
        Ballot {
            cur_term: 1,
            voted_for: Some(id.clone())
        }
    );

    // Tag
    let tag: crate::SnapshotTag = vec![].into();
    assert!(s.get_tag(10).await?.is_none());
    s.put_tag(10, tag.clone()).await?;
    assert_eq!(s.get_tag(10).await?, Some(tag.clone()));

    assert_eq!(s.get_snapshot_index().await?, 0);
    assert_eq!(s.get_last_index().await?, 0);
    assert!(s.get_entry(1).await?.is_none());

    let sn1 = e.clone();
    let e2 = e.clone();
    let e3 = e.clone();
    let e4 = e.clone();
    let e5 = e.clone();
    s.insert_snapshot(1, sn1).await?;
    assert_eq!(s.get_last_index().await?, 1);
    assert_eq!(s.get_snapshot_index().await?, 1);
    s.insert_entry(2, e2).await?;
    s.insert_entry(3, e3).await?;
    s.insert_entry(4, e4).await?;
    s.insert_entry(5, e5).await?;
    assert_eq!(s.get_last_index().await?, 5);

    let sn4 = e.clone();
    s.insert_snapshot(4, sn4).await?;
    assert_eq!(s.get_snapshot_index().await?, 4);
    let sn2 = e.clone();
    s.insert_snapshot(2, sn2).await?;
    assert_eq!(s.get_snapshot_index().await?, 4);

    assert!(s.get_entry(1).await?.is_some());
    s.delete_before(4).await?;
    assert!(s.get_entry(1).await?.is_none());

    Ok(())
}
