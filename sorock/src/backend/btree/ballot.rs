use anyhow::Ok;

use crate::process::{Ballot, RaftBallotStore};

use super::*;

pub struct BallotStore {
    ballot: Mutex<Ballot>,
}

impl BallotStore {
    pub fn new() -> BallotStore {
        Self {
            ballot: Mutex::new(Ballot::new()),
        }
    }
}

#[async_trait]
impl RaftBallotStore for BallotStore {
    async fn save_ballot(&self, _v: Ballot) -> Result<()> {
        // let mut balot = self.ballot;
        let mut ballot = self.ballot.lock();
        *ballot = _v;
        Ok(())
    }

    async fn load_ballot(&self) -> Result<Ballot> {
        let ballot = self.ballot.lock();
        Ok(ballot.clone())
    }
}
