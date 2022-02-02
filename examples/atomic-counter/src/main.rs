use atomic_counter::{Rep, Req};
use bytes::Bytes;
use lol_core::snapshot::{bytes::BytesSnapshot, SnapshotTag};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::RwLock;

#[derive(serde::Serialize, serde::Deserialize, PartialEq, Eq, Hash, Clone)]
pub struct Tag(pub Bytes);
impl Tag {
    pub fn new_unique() -> Self {
        // I know this isn't actually unique. But this is only an example.
        // Please use unique identifier as a tag.
        let x: u64 = rand::random();
        let x = bincode::serialize(&x).unwrap();
        Tag(x.into())
    }
}

#[derive(serde::Serialize, serde::Deserialize, Clone)]
pub struct Resource(pub u64);

struct MyApp {
    v: AtomicU64,
    snapshot_inventory: Arc<RwLock<HashMap<Tag, Resource>>>,
}
impl MyApp {
    pub fn new() -> Self {
        Self {
            v: AtomicU64::new(0),
            snapshot_inventory: Arc::new(RwLock::new(HashMap::new())),
        }
    }
}
#[tonic::async_trait]
impl lol_core::RaftApp for MyApp {
    async fn process_read(&self, request: &[u8]) -> anyhow::Result<Vec<u8>> {
        let req = Req::deserialize(&request).unwrap();
        match req {
            Req::Get => {
                let v = self.v.load(Ordering::SeqCst);
                let rep = Rep::Get(v);
                let rep = Rep::serialize(&rep);
                Ok(rep)
            }
            _ => unreachable!(),
        }
    }

    async fn process_write(
        &self,
        request: &[u8],
        apply_index: lol_core::Index,
    ) -> anyhow::Result<(Vec<u8>, lol_core::MakeSnapshot)> {
        let req = bincode::deserialize(&request)?;
        match req {
            Req::IncAndGet => {
                let old_v = self.v.fetch_add(1, Ordering::SeqCst);
                let rep = Rep::IncAndGet(old_v);
                let rep = Rep::serialize(&rep);
                Ok((rep, lol_core::MakeSnapshot::None))
            }
            _ => unreachable!(),
        }
    }

    async fn install_snapshot(
        &self,
        snapshot: Option<&lol_core::snapshot::SnapshotTag>,
        apply_index: lol_core::Index,
    ) -> anyhow::Result<()> {
        match snapshot {
            None => {
                self.v.store(0, Ordering::SeqCst);
            }
            Some(x) => {
                let tag = Tag(x.contents.clone());
                let resource = self
                    .snapshot_inventory
                    .read()
                    .await
                    .get(&tag)
                    .unwrap()
                    .clone();
                self.v.store(resource.0, Ordering::SeqCst);
            }
        }
        Ok(())
    }

    async fn fold_snapshot(
        &self,
        old_snapshot: Option<&lol_core::snapshot::SnapshotTag>,
        requests: Vec<&[u8]>,
    ) -> anyhow::Result<lol_core::snapshot::SnapshotTag> {
        let mut acc = match old_snapshot {
            None => 0,
            Some(x) => {
                let tag = Tag(x.contents.clone());
                let resource = self
                    .snapshot_inventory
                    .read()
                    .await
                    .get(&tag)
                    .unwrap()
                    .clone();
                resource.0
            }
        };
        for req in requests {
            let x = Req::deserialize(&req).unwrap();
            match x {
                Req::IncAndGet => acc += 1,
                _ => unreachable!(),
            }
        }
        let new_tag = Tag::new_unique();
        let new_resource = Resource(acc);
        self.snapshot_inventory
            .write()
            .await
            .insert(new_tag.clone(), new_resource);
        Ok(SnapshotTag {
            contents: new_tag.0,
        })
    }

    async fn save_snapshot(
        &self,
        st: lol_core::snapshot::SnapshotStream,
        snapshot_index: lol_core::Index,
    ) -> anyhow::Result<lol_core::snapshot::SnapshotTag> {
        let b = BytesSnapshot::save_snapshot_stream(st).await?;
        let v: u64 = bincode::deserialize(&b.contents).unwrap();
        let resource = Resource(v);
        let tag = Tag::new_unique();
        self.snapshot_inventory
            .write()
            .await
            .insert(tag.clone(), resource);
        let tag = SnapshotTag { contents: tag.0 };
        Ok(tag)
    }

    async fn open_snapshot(
        &self,
        x: &lol_core::snapshot::SnapshotTag,
    ) -> lol_core::snapshot::SnapshotStream {
        let tag = Tag(x.contents.clone());
        let resource = self
            .snapshot_inventory
            .read()
            .await
            .get(&tag)
            .unwrap()
            .clone();
        let b: BytesSnapshot = BytesSnapshot {
            contents: bincode::serialize(&resource.0).unwrap().into(),
        };
        b.open_snapshot_stream().await
    }

    async fn delete_snapshot(&self, x: &lol_core::snapshot::SnapshotTag) -> anyhow::Result<()> {
        let tag = Tag(x.contents.clone());
        self.snapshot_inventory.write().await.remove(&tag);
        Ok(())
    }
}

fn main() {}
