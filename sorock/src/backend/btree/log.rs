use std::time::Duration;

use super::*;

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

struct LazyInsert {
    index: Index,
    inner: Entry,
    space: String,
    notifier: oneshot::Sender<()>,
}

#[derive(Clone)]
pub struct Sender {
    tx: crossbeam::channel::Sender<LazyInsert>,
}

#[derive(Clone)]
pub struct Reaper {
    log_store: Arc<Mutex<BTreeMap<String, LogStore>>>,
    rx: crossbeam::channel::Receiver<LazyInsert>,
}

impl Reaper {
    pub fn new(log_store: Arc<Mutex<BTreeMap<String, LogStore>>>) -> (Self, Sender) {
        let (tx, rx) = crossbeam::channel::unbounded();

        let tx = Sender { tx };
        let this = Self { log_store, rx };
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

        for e in elems {
            self.log_store
                .lock()
                .get(&e.space)
                .unwrap()
                .b_tree_map
                .lock()
                .insert(e.index, value::ser(e.inner));

            notifiers.push(e.notifier);
        }

        for notifier in notifiers {
            notifier.send(()).ok();
        }
        Ok(())
    }
}

#[derive(Clone)]
pub struct LogStore {
    b_tree_map: Arc<Mutex<BTreeMap<u64, Vec<u8>>>>,
    reaper_queue: crossbeam::channel::Sender<LazyInsert>,
    space: String,
}

impl LogStore {
    pub fn new(shard_id: u32, q: Sender) -> Result<Self> {
        let space = format!("log-{shard_id}");

        Ok(Self {
            b_tree_map: Arc::new(Mutex::new(BTreeMap::new())),
            reaper_queue: q.tx,
            space: space,
        })
    }
}

#[async_trait]
impl RaftLogStore for LogStore {
    async fn insert_entry(&self, i: Index, e: Entry) -> Result<()> {
        let (tx, rx) = oneshot::channel();

        let lazy_insert = LazyInsert {
            index: i,
            inner: e,
            space: self.space.clone(),
            notifier: tx,
        };

        self.reaper_queue
            .send(lazy_insert)
            .map_err(|e| anyhow::anyhow!("failed to queue an entry {:?}", e.to_string()))?;

        rx.await?;
        Ok(())
    }

    async fn delete_entries_before(&self, i: Index) -> Result<()> {
        let map = self.b_tree_map.clone();
        map.lock().retain(|k, _| k >= &i);
        Ok(())
    }

    async fn get_entry(&self, i: Index) -> Result<Option<Entry>> {
        match self.b_tree_map.lock().get(&i) {
            Some(entry) => Ok(Some(value::desr(entry))),
            None => Ok(None),
        }
    }

    async fn get_head_index(&self) -> Result<Index> {
        let map = self.b_tree_map.lock();
        let index = map.first_key_value().unwrap_or_else(|| {
            panic!("Ref map is null");
        });
        Ok(index.0.clone())
    }

    async fn get_last_index(&self) -> Result<Index> {
        let map = self.b_tree_map.lock();

        let last_entry = map.last_key_value().unwrap();
        Ok(last_entry.0.clone())
    }
}
