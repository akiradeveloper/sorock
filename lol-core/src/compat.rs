use crate::{RaftApp, Index, SnapshotTag};
use crate::snapshot::{BytesSnapshot, SnapshotStream};
use async_trait::async_trait;

/// similar to full-featured RaftApp but restricted:
/// the snapshot is not a snapshot tag but a snapshot resource serialized into bytes.
#[async_trait]
pub trait RaftAppCompat: Sync + Send + 'static {
    async fn process_message(&self, request: &[u8]) -> anyhow::Result<Vec<u8>>;
    async fn apply_message(&self, request: &[u8], apply_index: Index) -> anyhow::Result<(Vec<u8>, Option<Vec<u8>>)>;
    async fn install_snapshot(&self, snapshot: Option<&[u8]>, apply_index: Index) -> anyhow::Result<()>;
    async fn fold_snapshot(
        &self,
        old_snapshot: Option<&[u8]>,
        requests: Vec<&[u8]>,
    ) -> anyhow::Result<Vec<u8>>;
}
pub struct ToRaftApp<A: RaftAppCompat> {
    compat_app: A,
}
impl <A: RaftAppCompat> ToRaftApp<A> {
    pub fn new(compat_app: A) -> Self {
        Self {
            compat_app,
        }
    }
}
#[async_trait]
impl <A: RaftAppCompat> RaftApp for ToRaftApp<A> {
    async fn process_message(&self, request: &[u8]) -> anyhow::Result<Vec<u8>> {
        self.compat_app.process_message(request).await
    }
    async fn apply_message(&self, request: &[u8], apply_index: Index) -> anyhow::Result<(Vec<u8>, Option<SnapshotTag>)> {
        let (res, new_snapshot) = self.compat_app.apply_message(request, apply_index).await?;
        Ok((res, new_snapshot.map(|x| x.into())))
    }
    async fn install_snapshot(&self, snapshot: Option<&SnapshotTag>, apply_index: Index) -> anyhow::Result<()> {
        let y = snapshot.map(|x| x.contents.clone());
        self.compat_app.install_snapshot(y.as_deref(), apply_index).await
    }
    async fn fold_snapshot(
        &self,
        old_snapshot: Option<&SnapshotTag>,
        requests: Vec<&[u8]>,
    ) -> anyhow::Result<SnapshotTag> {
        let y = old_snapshot.map(|x| x.contents.clone());
        let new_snapshot = self.compat_app.fold_snapshot(y.as_deref(), requests).await?;
        Ok(new_snapshot.into())
    }
    async fn from_snapshot_stream(&self, st: SnapshotStream, _: Index) -> anyhow::Result<SnapshotTag> {
        let b = BytesSnapshot::from_snapshot_stream(st).await?;
        let tag = SnapshotTag { contents: b.contents };
        Ok(tag)
    }
    async fn to_snapshot_stream(&self, x: &SnapshotTag) -> SnapshotStream {
        let b: BytesSnapshot = BytesSnapshot { contents: x.contents.clone() };
        b.to_snapshot_stream().await
    }
    async fn delete_resource(&self, _: &SnapshotTag) -> anyhow::Result<()> {
        Ok(())
    }
}