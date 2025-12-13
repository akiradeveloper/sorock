use super::*;

use redb::ReadableDatabase;

mod value {
    use super::*;

    #[derive(Deserialize, Serialize)]
    struct OnDiskStruct {
        term: u64,
        voted_for: Option<sorock::NodeAddress>,
    }

    pub fn ser(x: Ballot) -> Vec<u8> {
        let x = OnDiskStruct {
            term: x.cur_term,
            voted_for: x.voted_for,
        };
        let bin = bincode::serialize(&x).unwrap();
        bin
    }

    pub fn desr(bin: &[u8]) -> Ballot {
        let x: OnDiskStruct = bincode::deserialize(bin).unwrap();
        Ballot {
            cur_term: x.term,
            voted_for: x.voted_for,
        }
    }
}

const BALLOT: &str = "ballot";

fn table_def(space: &str) -> TableDefinition<'_, u32, Vec<u8>> {
    TableDefinition::new(&space)
}

pub struct BallotStore {
    db: Arc<Database>,
    shard_index: u32,
}

impl BallotStore {
    pub fn new(db: Arc<Database>, shard_index: u32) -> Result<Self> {
        // Insert the initial value if not exists.
        let tx = db.begin_write()?;
        {
            let mut tbl = tx.open_table(table_def(BALLOT))?;
            if tbl.get(shard_index)?.is_none() {
                tbl.insert(shard_index, value::ser(Ballot::new()))?;
            }
        }
        tx.commit()?;

        Ok(Self { db, shard_index })
    }

    pub async fn save_ballot(&self, ballot: Ballot) -> Result<()> {
        let tx = self.db.begin_write()?;
        {
            let mut tbl = tx.open_table(table_def(BALLOT))?;
            tbl.insert(self.shard_index, value::ser(ballot))?;
        }
        tx.commit()?;
        Ok(())
    }

    pub async fn load_ballot(&self) -> Result<Ballot> {
        let tx = self.db.begin_read()?;
        let tbl = tx.open_table(table_def(BALLOT))?;
        match tbl.get(self.shard_index)? {
            Some(bin) => Ok(value::desr(&bin.value())),
            None => Err(anyhow::anyhow!("No ballot")),
        }
    }
}
