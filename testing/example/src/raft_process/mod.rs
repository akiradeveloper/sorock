use super::*;

use anyhow::ensure;
use bytes::Bytes;
use futures::TryStreamExt;
use sorock::process::*;
use spin::{Mutex, RwLock};
use std::collections::BTreeMap;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, Write};
use std::path::Path;

mod snapshot_io;

pub async fn new(
    snap_file: Option<impl AsRef<Path>>,
    log: impl RaftLogStore,
    ballot: impl RaftBallotStore,
    driver: sorock::service::raft::RaftHandle,
) -> Result<RaftProcess> {
    let app_main = AppMain::new(snap_file);
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

#[derive(serde::Serialize, serde::Deserialize)]
struct SnapshotTable {
    inner: BTreeMap<u64, AppState>,
}
impl SnapshotTable {
    fn new() -> Self {
        Self {
            inner: BTreeMap::new(),
        }
    }
    fn insert(&mut self, idx: u64, state: AppState) {
        self.inner.insert(idx, state);
    }
    fn get(&self, idx: &u64) -> Option<&AppState> {
        self.inner.get(idx)
    }
    fn delete_before(&mut self, idx: &u64) {
        let latter = self.inner.split_off(idx);
        self.inner = latter;
    }
    fn contains_key(&self, idx: &u64) -> bool {
        self.inner.contains_key(idx)
    }
    fn get_latest_snapshot(&self) -> u64 {
        let mut out = vec![];
        for (&i, _) in &self.inner {
            out.push(i);
        }
        out.sort();
        out.pop().unwrap_or(0)
    }
}

enum SnapshotsInner {
    Mem(SnapshotTable),
    Disk(File),
}
struct Snapshots {
    inner: SnapshotsInner,
}
impl Snapshots {
    fn new(file: Option<File>) -> Self {
        let inner = match file {
            Some(mut f) => {
                if f.metadata().unwrap().len() == 0 {
                    let snap = SnapshotTable::new();
                    let mut data = serde_json::to_vec(&snap).unwrap();
                    f.rewind().unwrap();
                    f.set_len(0).unwrap();
                    f.write_all(&mut data).unwrap();
                }

                SnapshotsInner::Disk(f)
            }
            None => SnapshotsInner::Mem(SnapshotTable::new()),
        };
        Self { inner }
    }
    fn contains_key(&mut self, idx: &u64) -> bool {
        match &mut self.inner {
            SnapshotsInner::Mem(m) => m.contains_key(idx),
            SnapshotsInner::Disk(f) => {
                f.rewind().unwrap();
                let data: SnapshotTable = serde_json::from_reader(f).unwrap();

                data.contains_key(idx)
            }
        }
    }
    fn insert(&mut self, idx: u64, state: AppState) {
        match &mut self.inner {
            SnapshotsInner::Mem(m) => {
                m.insert(idx, state);
            }
            SnapshotsInner::Disk(f) => {
                let mut snap: SnapshotTable = {
                    let mut data = vec![];
                    f.rewind().unwrap();
                    f.read_to_end(&mut data).unwrap();
                    serde_json::from_slice(&data).unwrap()
                };

                snap.insert(idx, state);

                let data = serde_json::to_vec(&snap).unwrap();
                f.set_len(0).unwrap();
                f.rewind().unwrap();
                f.write_all(&data).unwrap();
            }
        }
    }
    fn get(&mut self, idx: &u64) -> Option<AppState> {
        match &mut self.inner {
            SnapshotsInner::Mem(m) => m.get(idx).cloned(),
            SnapshotsInner::Disk(f) => {
                f.rewind().unwrap();
                let snap: SnapshotTable = serde_json::from_reader(f).unwrap();

                snap.get(idx).cloned()
            }
        }
    }
    fn delete_before(&mut self, idx: &u64) {
        match &mut self.inner {
            SnapshotsInner::Mem(m) => {
                m.delete_before(idx);
            }
            SnapshotsInner::Disk(f) => {
                let mut snap: SnapshotTable = {
                    let mut data = vec![];
                    f.rewind().unwrap();
                    f.read_to_end(&mut data).unwrap();
                    serde_json::from_slice(&data).unwrap()
                };

                snap.delete_before(idx);

                let data = serde_json::to_vec(&snap).unwrap();
                f.rewind().unwrap();
                f.set_len(0).unwrap();
                f.write_all(&data).unwrap();
            }
        }
    }
    fn get_latest_snapshot(&mut self) -> u64 {
        match &mut self.inner {
            SnapshotsInner::Mem(m) => m.get_latest_snapshot(),
            SnapshotsInner::Disk(f) => {
                f.rewind().unwrap();
                let snap: SnapshotTable = serde_json::from_reader(f).unwrap();

                snap.get_latest_snapshot()
            }
        }
    }
}

struct InnerState {
    state_index: u64,
    counter: u64,
}
struct AppMain {
    state: RwLock<InnerState>,
    snapshots: Mutex<Snapshots>,
}
impl AppMain {
    pub fn new(snap_file: Option<impl AsRef<Path>>) -> Self {
        let init_state = InnerState {
            state_index: 0,
            counter: 0,
        };
        let file = snap_file.map(|p| OpenOptions::new().read(true).write(true).open(p).unwrap());
        let snapshots = Snapshots::new(file);
        Self {
            state: RwLock::new(init_state),
            snapshots: Mutex::new(snapshots),
        }
    }
}
#[async_trait::async_trait]
impl RaftApp for AppMain {
    async fn process_write(&self, bytes: &[u8], entry_index: LogIndex) -> Result<Bytes> {
        let mut cur_state = self.state.write();

        let req = AppWriteRequest::deserialize(bytes);
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

        let req = AppReadRequest::deserialize(bytes);
        match req {
            AppReadRequest::MakeSnapshot => {
                let idx = cur_state.state_index;
                let mut snapshots = self.snapshots.lock();
                snapshots.insert(idx, AppState(cur_state.counter));
            }
            AppReadRequest::Read => {}
        };

        Ok(AppState(cur_state.counter).serialize())
    }

    async fn install_snapshot(&self, snapshot_index: LogIndex) -> Result<()> {
        ensure!(self.snapshots.lock().contains_key(&snapshot_index));
        let snapshot = self.snapshots.lock().get(&snapshot_index).unwrap();

        let mut cur_state = self.state.write();
        cur_state.state_index = snapshot_index;
        cur_state.counter = snapshot.0;

        Ok(())
    }

    async fn save_snapshot(&self, st: SnapshotStream, snapshot_index: LogIndex) -> Result<()> {
        let snap = AppSnapshot::from_stream(st).await;
        self.snapshots.lock().insert(snapshot_index, snap.0);
        Ok(())
    }

    async fn open_snapshot(&self, x: LogIndex) -> Result<SnapshotStream> {
        ensure!(self.snapshots.lock().contains_key(&x));
        let cur_state = self.snapshots.lock().get(&x).unwrap();
        let snap = AppSnapshot(cur_state);
        let st = snap.into_stream();
        Ok(st)
    }

    async fn delete_snapshots_before(&self, x: LogIndex) -> Result<()> {
        let mut snapshots = self.snapshots.lock();
        snapshots.delete_before(&x);
        Ok(())
    }

    async fn get_latest_snapshot(&self) -> Result<LogIndex> {
        let k = self.snapshots.lock().get_latest_snapshot();
        Ok(k)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_persistent_snap_table() {
        let tmp = tempfile::NamedTempFile::new().unwrap();
        let f = OpenOptions::new()
            .read(true)
            .write(true)
            .open(tmp.path())
            .unwrap();
        let mut snap = Snapshots::new(Some(f));
        for i in 1..=10 {
            snap.insert(i, AppState(i));
        }
        assert!(snap.contains_key(&3));
        assert_eq!(snap.get_latest_snapshot(), 10);
        assert_eq!(snap.get(&7).unwrap().0, 7);
        snap.delete_before(&5);
        assert!(snap.get(&4).is_none());
        assert!(snap.get(&5).is_some());
    }
}
