use crate::{Clock, Term, Index, Id};
use rocksdb::{DB, Options, WriteBatch, ColumnFamilyDescriptor};
use super::{Entry, Vote};
use std::path::{Path, PathBuf};
use std::cmp::Ordering;
use tokio::sync::Semaphore;
use std::time::Duration;

const CF_ENTRIES: &str = "entries";
const CF_CTRL: &str = "ctrl";
const SNAPSHOT_INDEX: &str = "snapshot_index";
const VOTE: &str = "vote";
const CMP: &str = "index_asc";

#[derive(serde::Serialize, serde::Deserialize)]
struct EntryB {
    append_time: u64,
    prev_clock: (u64, u64),
    this_clock: (u64, u64),
    command: Vec<u8>,
}
#[derive(serde::Serialize, serde::Deserialize)]
struct VoteB {
    term: u64,
    voted_for: Option<String>,
}
#[derive(serde::Serialize, serde::Deserialize)]
struct SnapshotIndexB(u64);

impl From<Vec<u8>> for Entry {
    fn from(x: Vec<u8>) -> Self {
        let x: EntryB = rmp_serde::from_slice(&x).unwrap();
        Entry {
            append_time: Duration::from_millis(x.append_time),
            prev_clock: x.prev_clock,
            this_clock: x.this_clock,
            command: x.command,
        }
    }
}
impl Into<Vec<u8>> for Entry {
    fn into(self) -> Vec<u8> {
        let x = EntryB {
            append_time: self.append_time.as_millis() as u64,
            prev_clock: self.prev_clock,
            this_clock: self.this_clock,
            command: self.command,
        };
        rmp_serde::to_vec(&x).unwrap()
    }
}

impl From<Vec<u8>> for Vote {
    fn from(x: Vec<u8>) -> Self {
        let x: VoteB = rmp_serde::from_slice(&x).unwrap();
        Vote {
            cur_term: x.term,
            voted_for: x.voted_for,
        }
    }
}
impl Into<Vec<u8>> for Vote {
    fn into(self) -> Vec<u8> {
        let x = VoteB {
            term: self.cur_term,
            voted_for: self.voted_for,
        };
        rmp_serde::to_vec(&x).unwrap()
    }
}

impl From<Vec<u8>> for SnapshotIndexB {
    fn from(x: Vec<u8>) -> Self {
        rmp_serde::from_slice(&x).unwrap()
    }
}
impl Into<Vec<u8>> for SnapshotIndexB {
    fn into(self) -> Vec<u8> {
        rmp_serde::to_vec(&self).unwrap()
    }
}

#[derive(serde::Serialize, serde::Deserialize, Debug)]
struct IndexKey(u64);
fn encode_index(i: Index) -> Vec<u8> {
    rmp_serde::to_vec(&IndexKey(i)).unwrap()
}
fn decode_index(s: &[u8]) -> Index {
    let x: IndexKey = rmp_serde::from_slice(s).unwrap();
    x.0
}
fn comparator_fn(x: &[u8], y: &[u8]) -> Ordering {
    let x: Index = decode_index(x);
    let y: Index = decode_index(y);
    x.cmp(&y)
}

pub struct StorageBuilder {
    path: PathBuf,
}
impl StorageBuilder {
    pub fn new(path: &Path) -> Self {
        StorageBuilder {
            path: path.to_owned(),
        }
    }
    pub fn destory(&self) {
        let mut opts = Options::default();
        DB::destroy(&opts, &self.path).unwrap();
    }
    pub fn create(&self) {
        let mut db_opts = Options::default();
        db_opts.create_if_missing(true);
        db_opts.create_missing_column_families(true);
        let mut opts = Options::default();
        opts.set_comparator(CMP, comparator_fn);
        let cf_descs = vec![
            ColumnFamilyDescriptor::new(CF_ENTRIES, opts),
            ColumnFamilyDescriptor::new(CF_CTRL, Options::default()),
        ];
        let mut db = DB::open_cf_descriptors(&db_opts, &self.path, cf_descs).unwrap();

        // let mut opts = Options::default();
        // opts.set_comparator("by_index_key", comparator_fn);
        // db.create_cf(CF_ENTRIES, &opts).unwrap();
        // db.create_cf(CF_CTRL, &Options::default()).unwrap();

        let initial_vote = Vote {
            cur_term: 0,
            voted_for: None,
        };
        let cf = db.cf_handle(CF_CTRL).unwrap();
        let b: Vec<u8> = initial_vote.into();
        db.put_cf(&cf, VOTE, b).unwrap();

        let b: Vec<u8> = SnapshotIndexB(0).into();
        db.put_cf(&cf, SNAPSHOT_INDEX, b).unwrap();
    }
    fn open_db(&self) -> DB {
        let db_opts = Options::default();
        let mut opts = Options::default();
        opts.set_comparator(CMP, comparator_fn);
        let cf_descs = vec![
            ColumnFamilyDescriptor::new(CF_ENTRIES, opts),
            ColumnFamilyDescriptor::new(CF_CTRL, Options::default()),
        ];
        DB::open_cf_descriptors(&db_opts, &self.path, cf_descs).unwrap()
    }
    pub fn open(&self) -> Storage {
        let db = self.open_db();
        Storage::new(db)
    }
}

pub struct Storage {
    db: DB,
    snapshot_lock: Semaphore,
}
impl Storage {
    fn new(db: DB) -> Self {
        Self {
            db,
            snapshot_lock: Semaphore::new(1),
        }
    }
}
#[async_trait::async_trait]
impl super::RaftStorage for Storage {
    async fn delete_before(&self, r: Index) {
        let cf = self.db.cf_handle(CF_ENTRIES).unwrap();
        self.db.delete_range_cf(cf, encode_index(0), encode_index(r)).unwrap();
    }
    async fn get_last_index(&self) -> Index {
        let cf = self.db.cf_handle(CF_ENTRIES).unwrap();
        let mut iter = self.db.raw_iterator_cf(cf);
        iter.seek_to_last();
        // the iterator is empty
        if !iter.valid() {
            return 0
        }
        let key = iter.key().unwrap();
        decode_index(key)
    }
    async fn insert_snapshot(&self, i: Index, e: Entry) {
        let token = self.snapshot_lock.acquire().await;
        let cur_snapshot_index = self.get_snapshot_index().await;
        if i > cur_snapshot_index {
            let cf1 = self.db.cf_handle(CF_ENTRIES).unwrap();
            let cf2 = self.db.cf_handle(CF_CTRL).unwrap();
            let mut batch = WriteBatch::default();
            let b: Vec<u8> = e.into();
            batch.put_cf(&cf1, encode_index(i), b);
            let b: Vec<u8> = SnapshotIndexB(i).into();
            batch.put_cf(&cf2, SNAPSHOT_INDEX, b);
            // should we set_sync true here? or WAL saves our data on crash?
            self.db.write(batch).unwrap();
        }
    }
    async fn insert_entry(&self, i: Index, e: Entry) {
        let cf = self.db.cf_handle(CF_ENTRIES).unwrap();
        let b: Vec<u8> = e.into();
        self.db.put_cf(&cf, encode_index(i), b).unwrap(); 
    }
    async fn get_entry(&self, i: Index) -> Option<Entry> {
        let cf = self.db.cf_handle(CF_ENTRIES).unwrap();
        let b: Option<Vec<u8>> = self.db.get_cf(&cf, encode_index(i)).unwrap();
        b.map(|x| x.into())
    }
    async fn get_snapshot_index(&self) -> Index {
        let cf = self.db.cf_handle(CF_CTRL).unwrap();
        let b = self.db.get_cf(&cf, SNAPSHOT_INDEX).unwrap().unwrap();
        let x: SnapshotIndexB = b.into();
        x.0
    }
    async fn store_vote(&self, v: Vote) {
        let cf = self.db.cf_handle(CF_CTRL).unwrap();
        let b: Vec<u8> = v.into();
        self.db.put_cf(&cf, VOTE, b);
    }
    async fn load_vote(&self) -> Vote {
        let cf = self.db.cf_handle(CF_CTRL).unwrap();
        let b = self.db.get_cf(&cf, VOTE).unwrap().unwrap();
        b.into()
    }
}

#[tokio::test]
async fn test_rocksdb_storage() {
    std::fs::create_dir("/tmp/lol");
    let path = Path::new("/tmp/lol/disk1.db");
    let builder = StorageBuilder::new(&path);
    builder.destory();
    builder.create();
    let s = builder.open();

    super::test_storage(s).await;

    builder.destory();
}

#[tokio::test]
async fn test_rocksdb_persistency() {
    use std::time::Instant;

    std::fs::create_dir("/tmp/lol");
    let path = Path::new("/tmp/lol/disk2.db");
    let builder = StorageBuilder::new(&path);
    builder.destory();
    builder.create();
    let s: Box<super::RaftStorage> = Box::new(builder.open());

    let e = Entry {
        append_time: Duration::new(0,0),
        prev_clock: (0,0),
        this_clock: (0,0),
        command: vec![]
    };

    s.insert_snapshot(1, e.clone()).await;
    s.insert_entry(2, e.clone()).await;
    s.insert_entry(3, e.clone()).await;
    s.insert_entry(4, e.clone()).await;
    s.insert_snapshot(3, e.clone()).await;

    drop(s);

    let s: Box<super::RaftStorage> = Box::new(builder.open());
    assert_eq!(s.load_vote().await, Vote { cur_term: 0, voted_for: None });
    assert_eq!(s.get_snapshot_index().await, 3);
    assert_eq!(s.get_last_index().await, 4);

    drop(s);

    builder.destory();
}