use super::{util, SnapshotStream};
use std::path::{Path, PathBuf};
use bytes::Bytes;

/// Basic snapshot type which is just a byte sequence.
pub struct BytesSnapshot {
    pub contents: Bytes,
}
impl AsRef<[u8]> for BytesSnapshot {
    fn as_ref(&self) -> &[u8] {
        &self.contents
    }
}
impl From<Vec<u8>> for BytesSnapshot {
    fn from(x: Vec<u8>) -> Self {
        BytesSnapshot { contents: x.into() }
    }
}
impl BytesSnapshot {
    pub async fn to_snapshot_stream(&self) -> SnapshotStream {
        let cursor = std::io::Cursor::new(self.contents.clone());
        Box::pin(util::into_snapshot_stream(cursor))
    }
}
impl BytesSnapshot {
    pub async fn from_snapshot_stream(st: SnapshotStream) -> anyhow::Result<Self> {
        let mut v: Vec<u8> = vec![];
        let cursor = std::io::Cursor::new(&mut v);
        util::read_snapshot_stream(cursor, st).await?;
        Ok(BytesSnapshot { contents: v.into() })
    }
}

/// A snapshot saved in a file.
/// Instead of bytes snapshot you may choose this to deal with
/// gigantic snapshot beyond system memory.
pub struct FileSnapshot {
    pub path: PathBuf,
}
impl FileSnapshot {
    pub async fn to_snapshot_stream(&self) -> SnapshotStream {
        let f = tokio::fs::File::open(&self.path).await.unwrap();
        Box::pin(util::into_snapshot_stream(f))
    }
}
impl FileSnapshot {
    pub async fn from_snapshot_stream(st: SnapshotStream, path: &Path) -> anyhow::Result<Self> {
        let f = tokio::fs::File::create(path).await?;
        util::read_snapshot_stream(f, st).await?;
        Ok(FileSnapshot {
            path: path.to_owned(),
        })
    }
}