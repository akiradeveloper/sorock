use super::*;

mod value {
    use super::*;

    #[derive(serde::Deserialize, serde::Serialize)]
    struct OnDiskStruct {
        term: u64,
        voted_for: Option<sorock::NodeId>,
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

fn table_def(space: &str) -> TableDefinition<(), Vec<u8>> {
    TableDefinition::new(&space)
}

pub struct BallotStore {
    db: Arc<Database>,
    space: String,
}

impl BallotStore {
    pub fn new(db: Arc<Database>, shard_id: u32) -> Result<Self> {
        let space = format!("ballot-{shard_id}");

        // Insert the initial value if not exists.
        let tx = db.begin_write()?;
        {
            let mut tbl = tx.open_table(table_def(&space))?;
            if tbl.is_empty()? {
                tbl.insert((), value::ser(Ballot::new()))?;
            }
        }
        tx.commit()?;

        Ok(Self { db, space })
    }
}

#[async_trait]
impl RaftBallotStore for BallotStore {
    async fn save_ballot(&self, ballot: Ballot) -> Result<()> {
        let tx = self.db.begin_write()?;
        {
            let mut tbl = tx.open_table(table_def(&self.space))?;
            tbl.insert((), value::ser(ballot))?;
        }
        tx.commit()?;
        Ok(())
    }

    async fn load_ballot(&self) -> Result<Ballot> {
        let tx = self.db.begin_read()?;
        let tbl = tx.open_table(table_def(&self.space))?;
        match tbl.get(())? {
            Some(bin) => Ok(value::desr(&bin.value())),
            None => Err(anyhow::anyhow!("No ballot")),
        }
    }
}
