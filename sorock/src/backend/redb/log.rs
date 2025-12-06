use super::*;

use redb::ReadableDatabase;
use std::time::Duration;

mod value {
    use super::*;

    #[derive(serde::Deserialize, serde::Serialize)]
    struct OnDiskStruct {
        prev_term: u64,
        cur_index: u64,
        cur_term: u64,
        command: bytes::Bytes,
    }

    pub fn ser(x: Entry) -> Vec<u8> {
        let x = OnDiskStruct {
            prev_term: x.prev_clock.term,
            cur_index: x.this_clock.index,
            cur_term: x.this_clock.term,
            command: x.command,
        };
        let bin = bincode::serialize(&x).unwrap();
        bin
    }

    pub fn desr(bin: &[u8]) -> Entry {
        let x: OnDiskStruct = bincode::deserialize(bin).unwrap();
        Entry {
            prev_clock: Clock {
                index: x.cur_index - 1,
                term: x.prev_term,
            },
            this_clock: Clock {
                index: x.cur_index,
                term: x.prev_term,
            },
            command: x.command,
        }
    }
}

fn table_def(space: &str) -> redb::TableDefinition<u64, Vec<u8>> {
    redb::TableDefinition::new(space)
}

struct LazyInsert {
    index: LogIndex,
    inner: Entry,
    space: String,
    notifier: oneshot::Sender<()>,
}

#[derive(Clone)]
pub struct Sender {
    tx: crossbeam::channel::Sender<LazyInsert>,
}

pub struct Reaper {
    db: Arc<redb::Database>,
    rx: crossbeam::channel::Receiver<LazyInsert>,
}
impl Reaper {
    pub fn new(db: Arc<redb::Database>) -> (Self, Sender) {
        let (tx, rx) = crossbeam::channel::unbounded();
        let tx = Sender { tx };
        let this = Self { db, rx };
        (this, tx)
    }

    pub fn reap(&self) -> Result<()> {
        let mut elems = vec![];

        // Blocked until the first element is received.
        let head = self.rx.recv_timeout(Duration::from_millis(100))?;
        elems.push(head);

        let n = self.rx.len();
        for _ in 0..n {
            let e = self.rx.try_recv().unwrap();
            elems.push(e);
        }

        let mut notifiers = vec![];

        let tx = self.db.begin_write()?;
        for e in elems {
            let mut tbl = tx.open_table(table_def(&e.space))?;
            tbl.insert(e.index, value::ser(e.inner))?;
            notifiers.push(e.notifier);
        }
        tx.commit()?;

        for notifier in notifiers {
            notifier.send(()).ok();
        }
        Ok(())
    }
}

pub struct LogStore {
    db: Arc<Database>,
    space: String,
    reaper_queue: crossbeam::channel::Sender<LazyInsert>,
}
impl LogStore {
    pub fn new(db: Arc<Database>, shard_id: u32, q: Sender) -> Result<Self> {
        let space = format!("log-{shard_id}");

        let tx = db.begin_write()?;
        {
            let _ = tx.open_table(table_def(&space))?;
        }
        tx.commit()?;

        Ok(Self {
            db,
            space,
            reaper_queue: q.tx,
        })
    }
}
#[async_trait]
impl RaftLogStore for LogStore {
    async fn insert_entry(&self, i: LogIndex, e: Entry) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        let e = LazyInsert {
            index: i,
            inner: e,
            space: self.space.clone(),
            notifier: tx,
        };
        self.reaper_queue
            .send(e)
            .map_err(|_| anyhow::anyhow!("failed to queue an entry"))?;
        rx.await?;
        Ok(())
    }

    async fn delete_entries_before(&self, i: LogIndex) -> Result<()> {
        let tx = self.db.begin_write()?;
        {
            let mut tbl = tx.open_table(table_def(&self.space))?;
            tbl.retain_in(..i, |_, _| false)?;
        }
        tx.commit()?;
        Ok(())
    }

    async fn get_entry(&self, i: LogIndex) -> Result<Option<Entry>> {
        let tx = self.db.begin_read()?;
        let tbl = tx.open_table(table_def(&self.space))?;
        match tbl.get(i)? {
            Some(bin) => Ok(Some(value::desr(&bin.value()))),
            None => Ok(None),
        }
    }

    async fn get_head_index(&self) -> Result<LogIndex> {
        let tx = self.db.begin_read()?;
        let tbl = tx.open_table(table_def(&self.space))?;
        let out = tbl.first()?;
        Ok(match out {
            Some((k, _)) => k.value(),
            None => 0,
        })
    }

    async fn get_last_index(&self) -> Result<LogIndex> {
        let tx = self.db.begin_read()?;
        let tbl = tx.open_table(table_def(&self.space))?;
        let out = tbl.last()?;
        Ok(match out {
            Some((k, _)) => k.value(),
            None => 0,
        })
    }
}
