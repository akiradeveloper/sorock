use crate::snapshot::{BytesSnapshot, FileSnapshot};
use crate::{Index, MakeSnapshot, RaftApp, SnapshotStream};
use ::bytes::Bytes;
use anyhow::Result;
use async_trait::async_trait;

/// Restricted RaftApp where snapshot is an explicit byte sequence.
#[async_trait]
pub trait RaftAppSimple: Sync + Send + 'static {
    async fn process_read(&self, request: &[u8]) -> Result<Vec<u8>>;
    async fn process_write(&self, request: &[u8]) -> Result<(Vec<u8>, Option<Vec<u8>>)>;
    async fn install_snapshot(&self, snapshot: Option<&[u8]>) -> Result<()>;
    async fn fold_snapshot(
        &self,
        old_snapshot: Option<&[u8]>,
        requests: Vec<&[u8]>,
    ) -> Result<Vec<u8>>;
}

/// The repository of the snapshot resources.
#[async_trait]
pub trait SnapshotRepository: Sync + Send + 'static {
    async fn save_snapshot_stream(&self, st: SnapshotStream, snapshot_index: Index) -> Result<()>;
    async fn open_snapshot_stream(&self, index: Index) -> Result<SnapshotStream>;
    async fn delete_snapshot(&self, index: Index) -> Result<()>;
}

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::RwLock;

/// In-memory implementation of `SnapshotRepository`.
pub struct BytesRepository {
    resources: Arc<RwLock<HashMap<Index, BytesSnapshot>>>,
}
impl BytesRepository {
    pub fn new() -> Self {
        Self {
            resources: Arc::new(RwLock::new(HashMap::new())),
        }
    }
}
#[async_trait]
impl SnapshotRepository for BytesRepository {
    async fn save_snapshot_stream(&self, st: SnapshotStream, snapshot_index: Index) -> Result<()> {
        let bin = BytesSnapshot::save_snapshot_stream(st).await.unwrap();
        self.resources.write().unwrap().insert(snapshot_index, bin);
        Ok(())
    }
    async fn open_snapshot_stream(&self, index: Index) -> Result<SnapshotStream> {
        let bin = self.resources.read().unwrap().get(&index).unwrap().clone();
        let st = bin.open_snapshot_stream().await;
        Ok(st)
    }
    async fn delete_snapshot(&self, index: Index) -> Result<()> {
        self.resources.write().unwrap().remove(&index);
        Ok(())
    }
}

use std::path::{Path, PathBuf};

/// Persistent implementation of `SnapshotRepository`.
pub struct FileRepository {
    root_dir: PathBuf,
}
impl FileRepository {
    pub fn destroy(root_dir: &Path) -> Result<()> {
        std::fs::remove_dir_all(root_dir).ok();
        Ok(())
    }
    /// Create the initial state.
    /// You should call `destory` before calling this function.
    pub fn create(root_dir: &Path) -> Result<()> {
        std::fs::create_dir(root_dir)?;
        Ok(())
    }
    pub fn open(root_dir: &Path) -> Result<Self> {
        Ok(Self {
            root_dir: root_dir.to_owned(),
        })
    }
    fn snapshot_path(&self, i: Index) -> PathBuf {
        self.root_dir.join(format!("{}", i))
    }
}
#[async_trait]
impl SnapshotRepository for FileRepository {
    async fn save_snapshot_stream(&self, st: SnapshotStream, snapshot_index: Index) -> Result<()> {
        let path = self.snapshot_path(snapshot_index);
        FileSnapshot::save_snapshot_stream(st, &path).await?;
        Ok(())
    }
    async fn open_snapshot_stream(&self, index: Index) -> Result<SnapshotStream> {
        let path = self.snapshot_path(index);
        let snap = FileSnapshot { path };
        snap.open_snapshot_stream().await
    }
    async fn delete_snapshot(&self, index: Index) -> Result<()> {
        let path = self.snapshot_path(index);
        tokio::fs::remove_file(&path).await.ok();
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serial_test::serial;
    #[test]
    #[serial]
    fn test_file_repository() {
        let path = Path::new("/tmp/lol-test-file-repo");
        FileRepository::destroy(&path).unwrap();
        FileRepository::create(&path).unwrap();
    }
}

/// ToRaftApp turns an instance of RaftAppSimple into
/// a RaftApp instance.
pub struct ToRaftApp {
    app: Box<dyn RaftAppSimple>,
    repo: Box<dyn SnapshotRepository>,
}
impl ToRaftApp {
    pub fn new(app: impl RaftAppSimple, store: impl SnapshotRepository) -> Self {
        Self {
            app: Box::new(app),
            repo: Box::new(store),
        }
    }
    async fn save_snapshot(&self, bin: Bytes, idx: Index) -> Result<()> {
        let bin = BytesSnapshot { contents: bin };
        let inp = bin.open_snapshot_stream().await;
        self.repo.save_snapshot_stream(inp, idx).await
    }
    async fn get_snapshot(&self, idx: Index) -> Result<Bytes> {
        let st = self.repo.open_snapshot_stream(idx).await?;
        let bin = BytesSnapshot::save_snapshot_stream(st).await?;
        Ok(bin.contents)
    }
}
#[async_trait]
impl RaftApp for ToRaftApp {
    async fn process_read(&self, request: &[u8]) -> Result<Vec<u8>> {
        self.app.process_read(request).await
    }
    async fn process_write(
        &self,
        request: &[u8],
        entry_index: Index,
    ) -> Result<(Vec<u8>, MakeSnapshot)> {
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
    async fn install_snapshot(&self, snapshot: Option<Index>) -> Result<()> {
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
    ) -> Result<()> {
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
    async fn save_snapshot(&self, st: SnapshotStream, idx: Index) -> Result<()> {
        let b = BytesSnapshot::save_snapshot_stream(st).await?;
        self.save_snapshot(b.contents, idx).await
    }
    async fn open_snapshot(&self, x: Index) -> Result<SnapshotStream> {
        self.repo.open_snapshot_stream(x).await
    }
    async fn delete_snapshot(&self, idx: Index) -> Result<()> {
        self.repo.delete_snapshot(idx).await
    }
}
