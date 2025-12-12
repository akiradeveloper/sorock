use super::*;

pub struct LazyInsert {
    pub index: u64,
    pub data: Vec<u8>,
    pub space: String,
    pub notifier: oneshot::Sender<()>,
}

#[derive(Clone)]
pub struct ReaperTx {
    pub tx: crossbeam::channel::Sender<LazyInsert>,
}

pub struct Reaper {
    db: Arc<redb::Database>,
    rx: crossbeam::channel::Receiver<LazyInsert>,
}
impl Reaper {
    pub fn new(db: Arc<redb::Database>) -> (Self, ReaperTx) {
        let (tx, rx) = crossbeam::channel::unbounded();
        let tx = ReaperTx { tx };
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
            tbl.insert(e.index, e.data)?;
            notifiers.push(e.notifier);
        }
        tx.commit()?;

        for notifier in notifiers {
            notifier.send(()).ok();
        }
        Ok(())
    }
}
