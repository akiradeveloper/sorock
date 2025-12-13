use super::*;

use redb::{ReadableDatabase, ReadableTable};

pub struct LogShardView {
    db: Arc<redb::Database>,
    shard_index: u32,
    reaper_queue: crossbeam::channel::Sender<LazyInsert>,
}

impl LogShardView {
    pub(super) fn new(
        db: Arc<redb::Database>,
        shard_index: u32,
        q: crossbeam::channel::Sender<LazyInsert>,
    ) -> Result<Self> {
        let tx = db.begin_write()?;
        {
            let _ = tx.open_table(table_def(LOG))?;
        }
        tx.commit()?;

        Ok(Self {
            db,
            shard_index,
            reaper_queue: q,
        })
    }

    pub async fn insert_entry(&self, i: u64, e: Vec<u8>) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        let e = LazyInsert {
            shard_index: self.shard_index,
            index: i,
            data: e,
            notifier: tx,
        };
        self.reaper_queue
            .send(e)
            .map_err(|_| anyhow::anyhow!("failed to queue an entry"))?;
        rx.await?;
        Ok(())
    }

    pub async fn delete_entries_before(&self, i: u64) -> Result<()> {
        let tx = self.db.begin_write()?;
        {
            let mut tbl = tx.open_table(table_def(LOG))?;
            let start = (self.shard_index, 0);
            let end = (self.shard_index, i);
            tbl.retain_in(start..end, |_, _| false)?;
        }
        tx.commit()?;
        Ok(())
    }

    pub async fn get_entry(&self, i: u64) -> Result<Option<Vec<u8>>> {
        let tx = self.db.begin_read()?;
        let tbl = tx.open_table(table_def(LOG))?;
        match tbl.get((self.shard_index, i))? {
            Some(bin) => Ok(Some(bin.value())),
            None => Ok(None),
        }
    }

    pub async fn get_head_index(&self) -> Result<u64> {
        let tx = self.db.begin_read()?;
        let tbl = tx.open_table(table_def(LOG))?;

        let start = (self.shard_index, 0);
        let end = (self.shard_index + 1, 0);
        let mut range = tbl.range(start..end)?;

        let out = range.next();
        match out {
            Some(Ok(k)) => Ok(k.0.value().1),
            Some(_) => Err(anyhow::anyhow!("failed to get head index")),
            _ => Ok(0),
        }
    }

    pub async fn get_last_index(&self) -> Result<u64> {
        let tx = self.db.begin_read()?;
        let tbl = tx.open_table(table_def(LOG))?;

        let start = (self.shard_index, 0);
        let end = (self.shard_index + 1, 0);
        let range = tbl.range(start..end)?;

        let out = range.last();
        match out {
            Some(Ok(k)) => Ok(k.0.value().1),
            Some(_) => Err(anyhow::anyhow!("failed to get last index")),
            _ => Ok(0),
        }
    }
}
