use tokio::time::DelayQueue;
use tokio::sync::{RwLock, Mutex};
use std::sync::Arc;
use crate::{RaftCore, RaftApp, Index};
use std::collections::BTreeMap;
use crate::storage::Entry;
use std::time::Duration;
use futures::StreamExt;
use async_trait::async_trait;

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
            raft_core.log.insert_snapshot(e).await;
        }
    }
}

pub(crate) struct SnapshotInventory<SS> {
    m: RwLock<BTreeMap<Index, Arc<SS>>>,
}
impl <SS> SnapshotInventory<SS> {
    pub fn new() -> Self {
        Self {
            m: RwLock::new(BTreeMap::new()),
        }
    }
    pub async fn put(&self, i: Index, x: SS) {
        self.m.write().await.insert(i, Arc::new(x));
    }
    pub async fn get(&self, i: Index) -> Option<Arc<SS>> {
        self.m.read().await.get(&i).cloned()
    }
    pub async fn delete_before(&self, r: Index) {
        let ls: Vec<u64> = self.m.read().await.range(..r).map(|x| *x.0).collect();
        for i in ls {
            self.m.write().await.remove(&i);
        }
    }
}

use futures::stream::Stream;
use crate::protoimpl::GetSnapshotRep;
use bytes::Bytes;
/// the stream type that is used internally. it is considered as just a stream of bytes.
/// the length of each bytes may vary.
pub type SnapshotStream = std::pin::Pin<Box<dyn futures::stream::Stream<Item = anyhow::Result<Bytes>> + Send + Sync>>;
pub(crate) type SnapshotStreamOut = std::pin::Pin<Box<dyn futures::stream::Stream<Item = Result<GetSnapshotRep, tonic::Status>> + Send + Sync>>;
pub(crate) fn map_out(st: SnapshotStream) ->  SnapshotStreamOut {
    Box::pin(st.map(|res| res.map(|x| GetSnapshotRep { chunk: x.to_vec() }).map_err(|_| tonic::Status::unknown("streaming error"))))
}
pub(crate) fn map_in(st: impl Stream<Item = Result<GetSnapshotRep, tonic::Status>>) -> impl Stream<Item = anyhow::Result<Bytes>> {
    st.map(|res| res.map(|x| x.chunk.into()).map_err(|_| anyhow::Error::msg("streaming error")))
}
#[async_trait]
pub trait ToSnapshotStream: Sync + Send {
    async fn to_snapshot_stream(&self) -> SnapshotStream;
}
#[async_trait]
pub trait FromSnapshotStream {
    async fn from_snapshot_stream(st: SnapshotStream) -> Self;
}
/// basic snapshot type which is just a byte sequence.
pub struct BytesSnapshot(Bytes);
impl AsRef<[u8]> for BytesSnapshot {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}
impl From<Vec<u8>> for BytesSnapshot {
    fn from(x: Vec<u8>) -> Self {
        BytesSnapshot(x.into())
    }
}
#[async_trait]
impl ToSnapshotStream for BytesSnapshot {
    async fn to_snapshot_stream(&self) -> SnapshotStream {
        let cursor = std::io::Cursor::new(self.0.clone());
        Box::pin(util::into_snapshot_stream(cursor))
    }
}
#[async_trait]
impl FromSnapshotStream for BytesSnapshot {
    async fn from_snapshot_stream(st: SnapshotStream) -> Self {
        let mut v: Vec<u8> = vec![];
        let cursor = std::io::Cursor::new(&mut v);
        util::read_snapshot_stream(cursor, st).await;
        BytesSnapshot(v.into())
    }
}
/// a snapshot saved in a file.
/// instead of bytes snapshot you may choose this to deal with
/// gigantic snapshot beyond system memory.
struct FileSnapshot(pub std::path::PathBuf);
#[async_trait]
impl ToSnapshotStream for FileSnapshot {
    async fn to_snapshot_stream(&self) -> SnapshotStream {
        let f = tokio::fs::File::open(&self.0).await.unwrap();
        Box::pin(util::into_snapshot_stream(f))
    }
}
#[async_trait]
impl FromSnapshotStream for FileSnapshot {
    async fn from_snapshot_stream(st: SnapshotStream) -> Self {
        let path = std::path::Path::new("tmp"); // TODO make the file in unique path
        let f = tokio::fs::File::create(&path).await.unwrap();
        util::read_snapshot_stream(f, st).await;
        FileSnapshot(path.to_owned())
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
    async fn read_bytes_stream<W: AsyncWrite + Unpin>(w: W, mut st: impl Stream<Item=Result<Bytes>> + Unpin) {
        use futures::SinkExt;
        let mut sink = codec::FramedWrite::new(w, codec::BytesCodec::new());
        sink.send_all(&mut st).await;
    }
    pub async fn read_snapshot_stream<W: AsyncWrite + Unpin>(writer: W, st: impl Stream<Item=anyhow::Result<Bytes>> + Unpin) {
        let st = st.map(|res| res.map_err(|_| std::io::Error::from(std::io::ErrorKind::Other)));
        read_bytes_stream(writer, st).await;
    }
}
