use super::RaftStorage;
use super::{Ballot, Entry};
use crate::{Clock, Command, Id};
use anyhow::Result;

#[derive(serde::Serialize, serde::Deserialize)]
struct EntryB {
    prev_clock: (u64, u64),
    this_clock: (u64, u64),
    command: bytes::Bytes,
}
#[derive(serde::Serialize, serde::Deserialize)]
struct BallotB {
    term: u64,
    voted_for: Option<Id>,
}
#[derive(serde::Serialize, serde::Deserialize)]
struct SnapshotIndexB(u64);

impl <B: AsRef<[u8]>> From<B> for Entry {
    fn from(x: B) -> Self {
        let x: EntryB = bincode::deserialize(x.as_ref()).unwrap();
        Entry {
            prev_clock: Clock {
                term: x.prev_clock.0,
                index: x.prev_clock.1,
            },
            this_clock: Clock {
                term: x.this_clock.0,
                index: x.this_clock.1,
            },
            command: x.command.into(),
        }
    }
}
impl Into<Vec<u8>> for Entry {
    fn into(self) -> Vec<u8> {
        let x = EntryB {
            prev_clock: (self.prev_clock.term, self.prev_clock.index),
            this_clock: (self.this_clock.term, self.this_clock.index),
            command: self.command,
        };
        bincode::serialize(&x).unwrap()
    }
}

impl <B: AsRef<[u8]>> From<B> for Ballot {
    fn from(x: B) -> Self {
        let x: BallotB = bincode::deserialize(x.as_ref()).unwrap();
        Ballot {
            cur_term: x.term,
            voted_for: x.voted_for,
        }
    }
}
impl Into<Vec<u8>> for Ballot {
    fn into(self) -> Vec<u8> {
        let x = BallotB {
            term: self.cur_term,
            voted_for: self.voted_for,
        };
        bincode::serialize(&x).unwrap()
    }
}

pub(crate) async fn test_pre_close(s: impl RaftStorage) -> Result<()> {
    use std::collections::HashSet;

    let e = Entry {
        prev_clock: Clock { term: 0, index: 0 },
        this_clock: Clock { term: 0, index: 0 },
        command: Command::serialize(&Command::Noop),
    };
    let sn = Entry {
        prev_clock: Clock { term: 0, index: 0 },
        this_clock: Clock { term: 0, index: 0 },
        command: Command::serialize(&Command::Snapshot {
            membership: HashSet::new(),
        }),
    };
    s.insert_entry(1, sn.clone()).await?;
    s.insert_entry(2, e.clone()).await?;
    s.insert_entry(3, e.clone()).await?;
    s.insert_entry(4, e.clone()).await?;
    s.insert_entry(3, sn.clone()).await?;
    s.save_ballot(Ballot {
        cur_term: 1,
        voted_for: None,
    })
    .await?;
    Ok(())
}

pub(crate) async fn test_post_close(s: impl RaftStorage) -> Result<()> {
    assert_eq!(
        s.load_ballot().await?,
        Ballot {
            cur_term: 1,
            voted_for: None,
        }
    );
    assert_eq!(super::find_last_snapshot_index(&s).await?, Some(3));
    assert_eq!(s.get_last_index().await?, 4);
    Ok(())
}
