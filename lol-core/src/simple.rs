use crate::snapshot::{impls::BytesSnapshot, SnapshotStream, SnapshotTag};
use crate::{Index, MakeSnapshot, RaftApp};
use async_trait::async_trait;

/// Similar to full-featured RaftApp but restricted:
/// The snapshot is not a snapshot tag but a snapshot resource serialized into bytes.
#[async_trait]
pub trait RaftAppSimple: Sync + Send + 'static {
    async fn process_read(&self, request: &[u8]) -> anyhow::Result<Vec<u8>>;
    async fn process_write(
        &self,
        request: &[u8],
        apply_index: Index,
    ) -> anyhow::Result<(Vec<u8>, Option<Vec<u8>>)>;
    async fn install_snapshot(
        &self,
        snapshot: Option<&[u8]>,
        apply_index: Index,
    ) -> anyhow::Result<()>;
    async fn fold_snapshot(
        &self,
        old_snapshot: Option<&[u8]>,
        requests: Vec<&[u8]>,
    ) -> anyhow::Result<Vec<u8>>;
}
/// ToRaftApp turns an instance of RaftAppCompat into
/// RaftApp instance.
pub struct ToRaftApp<A: RaftAppSimple> {
    compat_app: A,
}
impl<A: RaftAppSimple> ToRaftApp<A> {
    pub fn new(compat_app: A) -> Self {
        Self { compat_app }
    }
}
#[async_trait]
impl<A: RaftAppSimple> RaftApp for ToRaftApp<A> {
    async fn process_read(&self, request: &[u8]) -> anyhow::Result<Vec<u8>> {
        self.compat_app.process_read(request).await
    }
    async fn process_write(
        &self,
        request: &[u8],
        apply_index: Index,
    ) -> anyhow::Result<(Vec<u8>, MakeSnapshot)> {
        let (res, new_snapshot) = self.compat_app.process_write(request, apply_index).await?;
        let make_snapshot = match new_snapshot {
            Some(x) => MakeSnapshot::CopySnapshot(x.into()),
            None => MakeSnapshot::None,
        };
        Ok((res, make_snapshot))
    }
    async fn install_snapshot(
        &self,
        snapshot: Option<&SnapshotTag>,
        apply_index: Index,
    ) -> anyhow::Result<()> {
        let y = snapshot.map(|x| x.contents.clone());
        self.compat_app
            .install_snapshot(y.as_deref(), apply_index)
            .await
    }
    async fn fold_snapshot(
        &self,
        old_snapshot: Option<&SnapshotTag>,
        requests: Vec<&[u8]>,
    ) -> anyhow::Result<SnapshotTag> {
        let y = old_snapshot.map(|x| x.contents.clone());
        let new_snapshot = self
            .compat_app
            .fold_snapshot(y.as_deref(), requests)
            .await?;
        Ok(new_snapshot.into())
    }
    async fn save_snapshot(&self, st: SnapshotStream, _: Index) -> anyhow::Result<SnapshotTag> {
        let b = BytesSnapshot::from_snapshot_stream(st).await?;
        let tag = SnapshotTag {
            contents: b.contents,
        };
        Ok(tag)
    }
    async fn open_snapshot(&self, x: &SnapshotTag) -> SnapshotStream {
        let b: BytesSnapshot = BytesSnapshot {
            contents: x.contents.clone(),
        };
        b.to_snapshot_stream().await
    }
    async fn delete_snapshot(&self, _: &SnapshotTag) -> anyhow::Result<()> {
        Ok(())
    }
}
