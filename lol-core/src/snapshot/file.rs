use super::{util, SnapshotStream};
use bytes::Bytes;
use std::path::{Path, PathBuf};

pub struct FileSnapshot {
    pub path: PathBuf,
}
impl FileSnapshot {
    pub async fn open_snapshot_stream(&self) -> SnapshotStream {
        let f = tokio::fs::File::open(&self.path).await.unwrap();
        Box::pin(util::into_snapshot_stream(f))
    }
}
impl FileSnapshot {
    pub async fn save_snapshot_stream(st: SnapshotStream, path: &Path) -> anyhow::Result<Self> {
        let f = tokio::fs::File::create(path).await?;
        util::read_snapshot_stream(f, st).await?;
        Ok(FileSnapshot {
            path: path.to_owned(),
        })
    }
}