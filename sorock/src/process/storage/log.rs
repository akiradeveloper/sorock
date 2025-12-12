use super::*;

mod value {
    use super::*;

    #[derive(Deserialize, Serialize)]
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

pub struct LogStore {
    view: LogShardView,
}
impl LogStore {
    pub fn new(view: LogShardView) -> Self {
        Self { view }
    }

    pub async fn insert_entry(&self, i: LogIndex, e: Entry) -> Result<()> {
        self.view.insert_entry(i, value::ser(e)).await?;

        Ok(())
    }

    pub async fn delete_entries_before(&self, i: LogIndex) -> Result<()> {
        self.view.delete_entries_before(i).await?;

        Ok(())
    }

    pub async fn get_entry(&self, i: LogIndex) -> Result<Option<Entry>> {
        self.view.get_entry(i).await.map(|bin0| {
            bin0.map(|bin| {
                let entry = value::desr(&bin);
                entry
            })
        })
    }

    pub async fn get_head_index(&self) -> Result<LogIndex> {
        self.view.get_head_index().await
    }

    pub async fn get_last_index(&self) -> Result<LogIndex> {
        self.view.get_last_index().await
    }
}
