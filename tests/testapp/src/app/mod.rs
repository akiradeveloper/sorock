use super::*;

use anyhow::ensure;
use bytes::Bytes;
use futures::{StreamExt, TryStreamExt};
use lol2::process::*;
use std::any;
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::RwLock;
use testapp::{AppReadRequest, AppState, AppWriteRequest};

mod snapshot_io;

pub async fn new(driver: lol2::RaftDriver) -> Result<RaftProcess> {
    let app_main = AppMain::new();
    let app_log = AppLog::new();
    let app_ballot = AppBallot::new();

    let process = RaftProcess::new(app_main, app_log, app_ballot, driver).await?;
    Ok(process)
}

struct AppSnapshot(AppState);
impl AppSnapshot {
    pub fn into_stream(self) -> SnapshotStream {
        let bytes = self.0.serialize();
        let cursor = std::io::Cursor::new(bytes);
        Box::pin(snapshot_io::read(cursor).map_err(|e| anyhow::anyhow!(e)))
    }

    pub async fn from_stream(st: SnapshotStream) -> Self {
        let mut v = vec![];
        let cursor = std::io::Cursor::new(&mut v);
        let st = st.map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e));
        snapshot_io::write(cursor, st).await.unwrap();
        let cur_state = AppState::deserialize(&v);
        AppSnapshot(cur_state)
    }
}

struct InnerState {
    index: u64,
    counter: u64,
}
struct AppMain {
    state: RwLock<InnerState>,
    snapshots: RwLock<BTreeMap<u64, AppState>>,
}
impl AppMain {
    pub fn new() -> Self {
        let init_state = InnerState {
            index: 0,
            counter: 0,
        };
        let snapshots = BTreeMap::new();
        Self {
            state: RwLock::new(init_state),
            snapshots: RwLock::new(snapshots),
        }
    }
}
#[async_trait::async_trait]
impl RaftApp for AppMain {
    async fn process_write(&self, bytes: &[u8], entry_index: Index) -> Result<Bytes> {
        let mut cur_state = self.state.write().unwrap();

        let req = testapp::AppWriteRequest::deserialize(bytes);
        let old_state = match req {
            AppWriteRequest::FetchAdd { bytes } => {
                let add_val = bytes.len() as u64;
                let n = cur_state.counter;
                cur_state.counter += add_val;
                n
            }
        };
        cur_state.index = entry_index;

        Ok(AppState(old_state).serialize())
    }

    async fn install_snapshot(&self, snapshot_index: Index) -> Result<()> {
        let snapshot = if snapshot_index == 1 {
            AppState(0)
        } else {
            ensure!(self.snapshots.read().unwrap().contains_key(&snapshot_index));
            *self.snapshots.read().unwrap().get(&snapshot_index).unwrap()
        };

        let mut cur_state = self.state.write().unwrap();
        cur_state.index = snapshot_index;
        cur_state.counter = snapshot.0;

        Ok(())
    }

    async fn process_read(&self, bytes: &[u8]) -> Result<Bytes> {
        let cur_state = self.state.read().unwrap();

        let req = testapp::AppReadRequest::deserialize(bytes);
        match req {
            AppReadRequest::MakeSnapshot => {
                let idx = cur_state.index;
                let mut snapshots = self.snapshots.write().unwrap();
                snapshots.insert(idx, AppState(cur_state.counter));
            }
            AppReadRequest::Read => {}
        };

        Ok(AppState(cur_state.counter).serialize())
    }

    async fn save_snapshot(&self, st: SnapshotStream, snapshot_index: Index) -> Result<()> {
        let snap = AppSnapshot::from_stream(st).await;
        self.snapshots
            .write()
            .unwrap()
            .insert(snapshot_index, snap.0);
        Ok(())
    }

    async fn open_snapshot(&self, x: Index) -> Result<SnapshotStream> {
        ensure!(self.snapshots.read().unwrap().contains_key(&x));
        let cur_state = *self.snapshots.read().unwrap().get(&x).unwrap();
        let snap = AppSnapshot(cur_state);
        let st = snap.into_stream();
        Ok(st)
    }

    async fn delete_snapshots_before(&self, x: Index) -> Result<()> {
        let mut snapshots = self.snapshots.write().unwrap();
        let latter = snapshots.split_off(&x);
        *snapshots = latter;
        Ok(())
    }

    async fn get_latest_snapshot(&self) -> Result<Index> {
        let k = {
            let mut out = vec![];
            let snapshots = self.snapshots.read().unwrap();
            for (&i, _) in snapshots.iter() {
                out.push(i);
            }
            out.sort();
            out.pop().unwrap_or(0)
        };
        Ok(k)
    }
}

struct AppBallot {
    inner: RwLock<Ballot>,
}
impl AppBallot {
    fn new() -> Self {
        Self {
            inner: RwLock::new(Ballot::new()),
        }
    }
}
#[async_trait::async_trait]
impl RaftBallotStore for AppBallot {
    async fn save_ballot(&self, v: Ballot) -> Result<()> {
        *self.inner.write().unwrap() = v;
        Ok(())
    }

    async fn load_ballot(&self) -> Result<Ballot> {
        let v = self.inner.read().unwrap().clone();
        Ok(v)
    }
}

struct AppLog {
    inner: RwLock<BTreeMap<Index, Entry>>,
}
impl AppLog {
    fn new() -> Self {
        Self {
            inner: RwLock::new(BTreeMap::new()),
        }
    }
}
#[async_trait::async_trait]
impl RaftLogStore for AppLog {
    async fn insert_entry(&self, i: Index, e: Entry) -> Result<()> {
        self.inner.write().unwrap().insert(i, e);
        Ok(())
    }

    async fn delete_entries_before(&self, i: Index) -> Result<()> {
        let mut inner = self.inner.write().unwrap();
        let latter = inner.split_off(&i);
        *inner = latter;
        Ok(())
    }

    async fn get_entry(&self, i: Index) -> Result<Option<Entry>> {
        let e: Option<Entry> = self.inner.read().unwrap().get(&i).cloned();
        Ok(e)
    }

    async fn get_head_index(&self) -> Result<Index> {
        let reader = self.inner.read().unwrap();
        let n = match reader.first_key_value() {
            Some((k, _)) => *k,
            None => 0,
        };
        Ok(n)
    }

    async fn get_last_index(&self) -> Result<Index> {
        let reader = self.inner.read().unwrap();
        let n = match reader.last_key_value() {
            Some((k, _)) => *k,
            None => 0,
        };
        Ok(n)
    }
}
