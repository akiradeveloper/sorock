use crate::snapshot::{BytesSnapshot, FileSnapshot, SnapshotStream};
use crate::{Index, MakeSnapshot, RaftApp};
use ::bytes::Bytes;
use async_trait::async_trait;

/// Similar to full-featured RaftApp but restricted:
/// The snapshot is not a snapshot tag but a snapshot resource serialized into bytes.
#[async_trait]
pub trait RaftAppSimple: Sync + Send + 'static {
    async fn process_read(&self, request: &[u8]) -> anyhow::Result<Vec<u8>>;
    async fn process_write(&self, request: &[u8]) -> anyhow::Result<(Vec<u8>, Option<Vec<u8>>)>;
    async fn install_snapshot(&self, snapshot: Option<&[u8]>) -> anyhow::Result<()>;
    async fn fold_snapshot(
        &self,
        old_snapshot: Option<&[u8]>,
        requests: Vec<&[u8]>,
    ) -> anyhow::Result<Vec<u8>>;
}

#[async_trait]
pub trait SnapshotStore: Sync + Send + 'static {
    async fn save_snapshot_stream(
        &self,
        st: SnapshotStream,
        snapshot_index: Index,
    ) -> anyhow::Result<()>;
    async fn open_snapshot_stream(&self, index: Index) -> anyhow::Result<SnapshotStream>;
    async fn delete_snapshot(&self, index: Index) -> anyhow::Result<()>;
}

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::RwLock;

pub struct BytesInventory {
    resources: Arc<RwLock<HashMap<Index, BytesSnapshot>>>,
}
impl BytesInventory {
    pub fn new() -> Self {
        Self {
            resources: Arc::new(RwLock::new(HashMap::new())),
        }
    }
}
#[async_trait]
impl SnapshotStore for BytesInventory {
    async fn save_snapshot_stream(
        &self,
        st: SnapshotStream,
        snapshot_index: Index,
    ) -> anyhow::Result<()> {
        let bin = BytesSnapshot::save_snapshot_stream(st).await.unwrap();
        self.resources.write().unwrap().insert(snapshot_index, bin);
        Ok(())
    }
    async fn open_snapshot_stream(&self, index: Index) -> anyhow::Result<SnapshotStream> {
        let bin = self.resources.read().unwrap().get(&index).unwrap().clone();
        let st = bin.open_snapshot_stream().await;
        Ok(st)
    }
    async fn delete_snapshot(&self, index: Index) -> anyhow::Result<()> {
        self.resources.write().unwrap().remove(&index);
        Ok(())
    }
}

use std::path::{Path, PathBuf};

pub struct FileInventory {
    root_dir: PathBuf,
}
impl FileInventory {
    pub fn destroy(root_dir: &Path) -> anyhow::Result<()> {
        std::fs::remove_dir_all(root_dir).ok();
        Ok(())
    }
    pub fn create(root_dir: &Path) -> anyhow::Result<()> {
        std::fs::create_dir(root_dir)?;
        Ok(())
    }
    pub fn new(root_dir: &Path) -> Self {
        Self {
            root_dir: root_dir.to_owned(),
        }
    }
    fn snapshot_path(&self, i: Index) -> PathBuf {
        self.root_dir.join(format!("{}", i))
    }
}
#[async_trait]
impl SnapshotStore for FileInventory {
    async fn save_snapshot_stream(
        &self,
        st: SnapshotStream,
        snapshot_index: Index,
    ) -> anyhow::Result<()> {
        let path = self.snapshot_path(snapshot_index);
        FileSnapshot::save_snapshot_stream(st, &path).await?;
        Ok(())
    }
    async fn open_snapshot_stream(&self, index: Index) -> anyhow::Result<SnapshotStream> {
        let path = self.snapshot_path(index);
        let snap = FileSnapshot { path };
        snap.open_snapshot_stream().await
    }
    async fn delete_snapshot(&self, index: Index) -> anyhow::Result<()> {
        let path = self.snapshot_path(index);
        tokio::fs::remove_file(&path).await.ok();
        Ok(())
    }
}
#[test]
fn test_file_inventory() {
    let path = Path::new("/tmp/lol-test-file-inventory");
    FileInventory::destroy(&path).unwrap();
    FileInventory::create(&path).unwrap();
}

/// ToRaftApp turns an instance of RaftAppSimple into
/// a RaftApp instance.
pub struct ToRaftApp {
    app: Box<dyn RaftAppSimple>,
    store: Box<dyn SnapshotStore>,
}
impl ToRaftApp {
    pub fn new(app: impl RaftAppSimple, store: impl SnapshotStore) -> Self {
        Self {
            app: Box::new(app),
            store: Box::new(store),
        }
    }
    async fn save_snapshot(&self, bin: Bytes, idx: Index) -> anyhow::Result<()> {
        let bin = BytesSnapshot { contents: bin };
        let inp = bin.open_snapshot_stream().await;
        self.store.save_snapshot_stream(inp, idx).await
    }
    async fn get_snapshot(&self, idx: Index) -> anyhow::Result<Bytes> {
        let st = self.store.open_snapshot_stream(idx).await?;
        let bin = BytesSnapshot::save_snapshot_stream(st).await?;
        Ok(bin.contents)
    }
}
#[async_trait]
impl RaftApp for ToRaftApp {
    async fn process_read(&self, request: &[u8]) -> anyhow::Result<Vec<u8>> {
        self.app.process_read(request).await
    }
    async fn process_write(
        &self,
        request: &[u8],
        entry_index: Index,
    ) -> anyhow::Result<(Vec<u8>, MakeSnapshot)> {
        let (res, new_snapshot) = self.app.process_write(request).await?;
        let make_snapshot = match new_snapshot {
            Some(x) => {
                let ok = self.save_snapshot(x.into(), entry_index).await.is_ok();
                if ok {
                    MakeSnapshot::CopySnapshot
                } else {
                    MakeSnapshot::None
                }
            }
            None => MakeSnapshot::None,
        };
        Ok((res, make_snapshot))
    }
    async fn install_snapshot(&self, snapshot: Option<Index>) -> anyhow::Result<()> {
        let snapshot = match snapshot {
            Some(idx) => Some(self.get_snapshot(idx).await?),
            None => None,
        };
        self.app.install_snapshot(snapshot.as_deref()).await
    }
    async fn fold_snapshot(
        &self,
        old_snapshot: Option<Index>,
        requests: Vec<&[u8]>,
        snapshot_index: Index,
    ) -> anyhow::Result<()> {
        let old_snapshot = match old_snapshot {
            Some(idx) => Some(self.get_snapshot(idx).await?),
            None => None,
        };
        let new_snapshot = self
            .app
            .fold_snapshot(old_snapshot.as_deref(), requests)
            .await?;
        self.save_snapshot(new_snapshot.into(), snapshot_index)
            .await
    }
    async fn save_snapshot(&self, st: SnapshotStream, idx: Index) -> anyhow::Result<()> {
        let b = BytesSnapshot::save_snapshot_stream(st).await?;
        self.save_snapshot(b.contents, idx).await
    }
    async fn open_snapshot(&self, x: Index) -> anyhow::Result<SnapshotStream> {
        self.store.open_snapshot_stream(x).await
    }
    async fn delete_snapshot(&self, idx: Index) -> anyhow::Result<()> {
        self.store.delete_snapshot(idx).await
    }
}
