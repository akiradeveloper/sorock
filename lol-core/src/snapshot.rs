use tokio::time::DelayQueue;
use tokio::sync::{RwLock, Mutex};
use std::sync::Arc;
use crate::{RaftCore, RaftApp};
use crate::storage::Entry;
use std::time::Duration;
use futures::StreamExt;
use std::path::{Path, PathBuf};

pub(crate) struct InsertSnapshot {
    pub e: Entry,
}
pub(crate) struct SnapshotQueue {
    q: Mutex<DelayQueue<InsertSnapshot>>,
}
impl SnapshotQueue {
    pub fn new() -> Self {
        Self {
            q: Mutex::new(DelayQueue::new()),
        }
    }
    pub async fn insert(&self, x: InsertSnapshot, delay: Duration) {
        self.q.lock().await.insert(x, delay);
    }
    pub async fn run_once<A: RaftApp>(&self, raft_core: Arc<RaftCore<A>>) {
        while let Some(Ok(expired)) = self.q.lock().await.next().await {
            let InsertSnapshot { e } = expired.into_inner();
            log::info!("insert new snapshot index = {}", e.this_clock.index);
            let _ = raft_core.log.insert_snapshot(e).await;
        }
    }
}

/// snapshot tag is a tag that bound to some snapshot resource.
/// if the resource is a file the tag is the path to the file, for example.
#[derive(Clone, Debug, PartialEq)]
pub struct SnapshotTag {
    pub contents: bytes::Bytes
}
impl AsRef<[u8]> for SnapshotTag {
    fn as_ref(&self) -> &[u8] {
        self.contents.as_ref()
    }
}
impl From<Vec<u8>> for SnapshotTag {
    fn from(x: Vec<u8>) -> SnapshotTag {
        SnapshotTag {
            contents: x.into()
        }
    }
}

use futures::stream::Stream;
use crate::proto_compiled::GetSnapshotRep;
use bytes::Bytes;
/// the stream type that is used internally. it is considered as just a stream of bytes.
/// the length of each bytes may vary.
pub type SnapshotStream = std::pin::Pin<Box<dyn futures::stream::Stream<Item = anyhow::Result<Bytes>> + Send>>;
pub(crate) type SnapshotStreamOut = std::pin::Pin<Box<dyn futures::stream::Stream<Item = Result<GetSnapshotRep, tonic::Status>> + Send + Sync>>;
pub(crate) fn into_out_stream(in_stream: SnapshotStream) ->  SnapshotStreamOut {
    let out_stream = in_stream.map(|res| res.map(|x| GetSnapshotRep { chunk: x.to_vec() }).map_err(|_| tonic::Status::unknown("streaming error")));
    Box::pin(crate::SyncStream::new(out_stream))
}
pub(crate) fn into_in_stream(out_stream: impl Stream<Item = Result<GetSnapshotRep, tonic::Status>>) -> impl Stream<Item = anyhow::Result<Bytes>> {
    out_stream.map(|res| res.map(|x| x.chunk.into()).map_err(|_| anyhow::Error::msg("streaming error")))
}
/// basic snapshot type which is just a byte sequence.
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
/// a snapshot saved in a file.
/// instead of bytes snapshot you may choose this to deal with
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
        Ok(FileSnapshot { path: path.to_owned() })
    }
}
mod util {
    use bytes::Bytes;
    use futures::stream::{Stream, StreamExt, TryStreamExt};
    use tokio::io::{AsyncRead, AsyncWrite, Result};
    use tokio_util::codec;
    fn into_bytes_stream<R>(r: R) -> impl Stream<Item=Result<Bytes>>
    where
        R: AsyncRead,
    {
        codec::FramedRead::new(r, codec::BytesCodec::new())
            .map_ok(|bytes| bytes.freeze())
    }
    pub fn into_snapshot_stream<R: AsyncRead>(reader: R) -> impl Stream<Item=anyhow::Result<Bytes>> {
        into_bytes_stream(reader).map(|res| res.map_err(|_| anyhow::Error::msg("streaming error")))
    }
    async fn read_bytes_stream<W: AsyncWrite + Unpin>(w: W, mut st: impl Stream<Item=Result<Bytes>> + Unpin) -> anyhow::Result<()> {
        use futures::SinkExt;
        let mut sink = codec::FramedWrite::new(w, codec::BytesCodec::new());
        sink.send_all(&mut st).await?;
        Ok(())
    }
    pub async fn read_snapshot_stream<W: AsyncWrite + Unpin>(writer: W, st: impl Stream<Item=anyhow::Result<Bytes>> + Unpin) -> anyhow::Result<()> {
        let st = st.map(|res| res.map_err(|_| std::io::Error::from(std::io::ErrorKind::Other)));
        read_bytes_stream(writer, st).await
    }
}
