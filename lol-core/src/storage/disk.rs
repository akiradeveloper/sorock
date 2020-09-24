use crate::{Clock, Term, Index, Id};
use rocksdb::{DB, Options, WriteBatch};
use crate::{Entry, Vote};

static CF_ENTRIES: &str = "entries";
static CF_CTRL: &str = "ctrl";

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
        unimplemented!()
    }
}
impl Into<Vec<u8>> for Entry {
    fn into(self) -> Vec<u8> {
        unimplemented!()
    }
}

impl From<Vec<u8>> for Vote {
    fn from(x: Vec<u8>) -> Self {
        unimplemented!()
    }
}
impl Into<Vec<u8>> for Vote {
    fn into(self) -> Vec<u8> {
        unimplemented!()
    }
}

impl From<Vec<u8>> for SnapshotIndexB {
    fn from(x: Vec<u8>) -> Self {
        unimplemented!()
    }
}
impl Into<Vec<u8>> for SnapshotIndexB {
    fn into(self) -> Vec<u8> {
        unimplemented!()
    }
}

struct Storage {
    db: DB,
}
impl Storage {
    pub fn new() -> Self {
        let mut db = DB::open_default("hoge").unwrap(); // tmp
        // what if creating cf twice for the same db?
        let opts = Options::default();
        db.create_cf(CF_ENTRIES, &opts).unwrap();
        db.create_cf(CF_CTRL, &opts).unwrap();
        Self {
            db,
        }
    }
    fn encode_index(i: Index) -> String {
        format!("{}", i)
    }
    fn decode_index(s: &[u8]) -> Index {
        unimplemented!()
    }
}
#[async_trait::async_trait]
impl super::RaftStorage for Storage {
    async fn delete_before(&self, r: Index) {
        let cf = self.db.cf_handle(CF_ENTRIES).unwrap();
        self.db.delete_range_cf(cf, Self::encode_index(0), Self::encode_index(r));
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
        Self::decode_index(key)
    }
    async fn insert_snapshot(&self, i: Index, e: Entry) {
        let cf1 = self.db.cf_handle(CF_ENTRIES).unwrap();
        let cf2 = self.db.cf_handle(CF_CTRL).unwrap();
        let mut batch = WriteBatch::default();
        let b: Vec<u8> = e.into();
        batch.put_cf(&cf1, Self::encode_index(i), b);
        let b: Vec<u8> = SnapshotIndexB(i).into();
        batch.put_cf(&cf2, "snapshot_index", b);
        // should we set_sync true here? or WAL saves our data on crash?
        self.db.write(batch);
    }
    async fn insert_entry(&self, i: Index, e: Entry) {
        unimplemented!()
    }
    async fn get_entry(&self, i: Index) -> Option<Entry> {
        unimplemented!()
    }
    async fn get_snapshot_index(&self) -> Index {
        unimplemented!()
    }
    async fn store_vote(&self, v: Vote) {
        let cf = self.db.cf_handle(CF_CTRL).unwrap();
        let b: Vec<u8> = v.into();
        self.db.put_cf(&cf, "vote", b);
    }
    async fn load_vote(&self) -> Vote {
        let cf = self.db.cf_handle(CF_CTRL).unwrap();
        let b = self.db.get_cf(&cf, "vote").unwrap().unwrap();
        b.into()
    }
}