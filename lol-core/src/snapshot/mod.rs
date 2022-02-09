use futures::StreamExt;

mod queue;
pub(crate) use queue::*;
mod util;

use anyhow::Result;
use bytes::Bytes;

use crate::proto_compiled::GetSnapshotRep;
use futures::stream::Stream;

use crate::SnapshotStream;

pub(crate) type SnapshotStreamOut = std::pin::Pin<
    Box<dyn futures::stream::Stream<Item = Result<GetSnapshotRep, tonic::Status>> + Send>,
>;

pub(crate) fn into_out_stream(in_stream: SnapshotStream) -> SnapshotStreamOut {
    let out_stream = in_stream.map(|res| {
        res.map(|x| GetSnapshotRep { chunk: x })
            .map_err(|_| tonic::Status::unknown("streaming error"))
    });
    Box::pin(out_stream)
}

pub(crate) fn into_in_stream(
    out_stream: impl Stream<Item = Result<GetSnapshotRep, tonic::Status>>,
) -> impl Stream<Item = anyhow::Result<Bytes>> {
    out_stream.map(|res| {
        res.map(|x| x.chunk.into())
            .map_err(|_| anyhow::Error::msg("streaming error"))
    })
}

/// Basic snapshot type that contains all the data in the byte sequence.
#[derive(Clone)]
pub(crate) struct BytesSnapshot {
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
    pub async fn open_snapshot_stream(&self) -> SnapshotStream {
        let cursor = std::io::Cursor::new(self.contents.clone());
        Box::pin(util::into_snapshot_stream(cursor))
    }
}
impl BytesSnapshot {
    pub async fn save_snapshot_stream(st: SnapshotStream) -> Result<Self> {
        let mut v: Vec<u8> = vec![];
        let cursor = std::io::Cursor::new(&mut v);
        util::read_snapshot_stream(cursor, st).await?;
        Ok(BytesSnapshot { contents: v.into() })
    }
}

use std::path::{Path, PathBuf};

/// Persistent snapshot type that stores all the data in a normal file.
pub(crate) struct FileSnapshot {
    pub path: PathBuf,
}
impl FileSnapshot {
    pub async fn open_snapshot_stream(&self) -> Result<SnapshotStream> {
        let f = tokio::fs::File::open(&self.path).await?;
        Ok(Box::pin(util::into_snapshot_stream(f)))
    }
}
impl FileSnapshot {
    pub async fn save_snapshot_stream(st: SnapshotStream, path: &Path) -> Result<Self> {
        let f = tokio::fs::File::create(path).await?;
        util::read_snapshot_stream(f, st).await?;
        Ok(FileSnapshot {
            path: path.to_owned(),
        })
    }
}
