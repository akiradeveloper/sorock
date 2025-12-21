use super::*;

use redb::{ReadableDatabase, ReadableTable};

pub struct LogShardView {
    db: Arc<redb::Database>,
    space: String,
    reaper_queue: crossbeam::channel::Sender<LazyInsert>,
}

impl LogShardView {
    pub(super) fn new(
        db: Arc<redb::Database>,
        shard_index: u32,
        q: crossbeam::channel::Sender<LazyInsert>,
    ) -> Result<Self> {
        let space = format!("log.{shard_index}");

        let tx = db.begin_write()?;
        {
            let _ = tx.open_table(table_def(&space))?;
        }
        tx.commit()?;

        Ok(Self {
            db,
            space,
            reaper_queue: q,
        })
    }

    pub async fn insert_entry(&self, i: u64, e: Vec<u8>) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        let e = LazyInsert {
            index: i,
            data: e,
            space: self.space.clone(),
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
            let mut tbl = tx.open_table(table_def(&self.space))?;
            tbl.retain_in(..i, |_, _| false)?;
        }
        tx.commit()?;
        Ok(())
    }

    pub async fn delete_entries_after(&self, i: u64) -> Result<()> {
        let tx = self.db.begin_write()?;
        {
            let mut tbl = tx.open_table(table_def(&self.space))?;
            tbl.retain_in((i + 1).., |_, _| false)?;
        }
        tx.commit()?;
        Ok(())
    }

    pub async fn get_entry(&self, i: u64) -> Result<Option<Vec<u8>>> {
        let tx = self.db.begin_read()?;
        let tbl = tx.open_table(table_def(&self.space))?;
        match tbl.get(i)? {
            Some(bin) => Ok(Some(bin.value())),
            None => Ok(None),
        }
    }

    pub async fn get_head_index(&self) -> Result<u64> {
        let tx = self.db.begin_read()?;
        let tbl = tx.open_table(table_def(&self.space))?;
        let out = tbl.first()?;
        Ok(match out {
            Some((k, _)) => k.value(),
            None => 0,
        })
    }

    pub async fn get_last_index(&self) -> Result<u64> {
        let tx = self.db.begin_read()?;
        let tbl = tx.open_table(table_def(&self.space))?;
        let out = tbl.last()?;
        Ok(match out {
            Some((k, _)) => k.value(),
            None => 0,
        })
    }
}
