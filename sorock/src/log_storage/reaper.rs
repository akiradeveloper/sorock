use super::*;

pub struct LazyInsert {
    pub shard_index: u32,
    pub index: u64,
    pub data: Vec<u8>,
    pub notifier: oneshot::Sender<()>,
}

pub struct Reaper {
    db: Arc<redb::Database>,
    rx: crossbeam::channel::Receiver<LazyInsert>,
}
impl Reaper {
    pub fn new(db: Arc<redb::Database>) -> (Self, crossbeam::channel::Sender<LazyInsert>) {
        let (tx, rx) = crossbeam::channel::unbounded();
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
        // Ordered insertion will improve performance.
        elems.sort_by_key(|e| (e.shard_index, e.index));

        let mut notifiers = vec![];

        let tx = self.db.begin_write()?;
        {
            let mut tbl = tx.open_table(table_def(LOG))?;
            for e in elems {
                tbl.insert((e.shard_index, e.index), e.data)?;
                notifiers.push(e.notifier);
            }
        }
        tx.commit()?;

        for notifier in notifiers {
            notifier.send(()).ok();
        }
        Ok(())
    }
}
