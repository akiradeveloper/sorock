use super::*;

use anyhow::ensure;
use bytes::Bytes;
use futures::TryStreamExt;
use lolraft::process::*;
use spin::RwLock;
use std::collections::BTreeMap;
use testapp::{AppReadRequest, AppState, AppWriteRequest};

mod snapshot_io;

pub async fn new(
    log: impl RaftLogStore,
    ballot: impl RaftBallotStore,
    driver: lolraft::RaftDriver,
) -> Result<RaftProcess> {
    let app_main = AppMain::new();
    let process = RaftProcess::new(app_main, log, ballot, driver).await?;
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
    state_index: u64,
    counter: u64,
}
struct AppMain {
    state: RwLock<InnerState>,
    snapshots: RwLock<BTreeMap<u64, AppState>>,
}
impl AppMain {
    pub fn new() -> Self {
        let init_state = InnerState {
            state_index: 0,
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
        let mut cur_state = self.state.write();

        let req = testapp::AppWriteRequest::deserialize(bytes);
        let old_state = match req {
            AppWriteRequest::FetchAdd { bytes } => {
                let add_val = bytes.len() as u64;
                let n = cur_state.counter;
                cur_state.counter += add_val;
                n
            }
        };
        cur_state.state_index = entry_index;

        Ok(AppState(old_state).serialize())
    }

    async fn process_read(&self, bytes: &[u8]) -> Result<Bytes> {
        let cur_state = self.state.read();

        let req = testapp::AppReadRequest::deserialize(bytes);
        match req {
            AppReadRequest::MakeSnapshot => {
                let idx = cur_state.state_index;
                let mut snapshots = self.snapshots.write();
                snapshots.insert(idx, AppState(cur_state.counter));
            }
            AppReadRequest::Read => {}
        };

        Ok(AppState(cur_state.counter).serialize())
    }

    async fn install_snapshot(&self, snapshot_index: Index) -> Result<()> {
        ensure!(self.snapshots.read().contains_key(&snapshot_index));
        let snapshot = *self.snapshots.read().get(&snapshot_index).unwrap();

        let mut cur_state = self.state.write();
        cur_state.state_index = snapshot_index;
        cur_state.counter = snapshot.0;

        Ok(())
    }

    async fn save_snapshot(&self, st: SnapshotStream, snapshot_index: Index) -> Result<()> {
        let snap = AppSnapshot::from_stream(st).await;
        self.snapshots.write().insert(snapshot_index, snap.0);
        Ok(())
    }

    async fn open_snapshot(&self, x: Index) -> Result<SnapshotStream> {
        ensure!(self.snapshots.read().contains_key(&x));
        let cur_state = *self.snapshots.read().get(&x).unwrap();
        let snap = AppSnapshot(cur_state);
        let st = snap.into_stream();
        Ok(st)
    }

    async fn delete_snapshots_before(&self, x: Index) -> Result<()> {
        let mut snapshots = self.snapshots.write();
        let latter = snapshots.split_off(&x);
        *snapshots = latter;
        Ok(())
    }

    async fn get_latest_snapshot(&self) -> Result<Index> {
        let k = {
            let mut out = vec![];
            let snapshots = self.snapshots.read();
            for (&i, _) in snapshots.iter() {
                out.push(i);
            }
            out.sort();
            out.pop().unwrap_or(0)
        };
        Ok(k)
    }
}
