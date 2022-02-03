use atomic_counter::{Rep, Req};
use bytes::Bytes;
use lol_core::snapshot::BytesSnapshot;
use lol_core::Index;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::RwLock;

#[derive(serde::Serialize, serde::Deserialize, Clone)]
struct Resource(u64);

struct MyApp {
    v: AtomicU64,
    snapshot_inventory: Arc<RwLock<HashMap<Index, Resource>>>,
}
impl MyApp {
    fn new() -> Self {
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
        apply_index: Index,
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

    async fn install_snapshot(&self, snapshot: Option<Index>) -> anyhow::Result<()> {
        match snapshot {
            None => {
                self.v.store(0, Ordering::SeqCst);
            }
            Some(x) => {
                let resource = self
                    .snapshot_inventory
                    .read()
                    .await
                    .get(&x)
                    .unwrap()
                    .clone();
                self.v.store(resource.0, Ordering::SeqCst);
            }
        }
        Ok(())
    }

    async fn fold_snapshot(
        &self,
        old_snapshot: Option<Index>,
        requests: Vec<&[u8]>,
        snapshot_index: Index,
    ) -> anyhow::Result<()> {
        let mut acc = match old_snapshot {
            None => 0,
            Some(x) => {
                let resource = self
                    .snapshot_inventory
                    .read()
                    .await
                    .get(&x)
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
        let new_resource = Resource(acc);
        self.snapshot_inventory
            .write()
            .await
            .insert(snapshot_index, new_resource);
        Ok(())
    }

    async fn save_snapshot(
        &self,
        st: lol_core::snapshot::SnapshotStream,
        snapshot_index: Index,
    ) -> anyhow::Result<()> {
        let b = BytesSnapshot::save_snapshot_stream(st).await?;
        let v: u64 = bincode::deserialize(&b.contents).unwrap();
        let resource = Resource(v);
        self.snapshot_inventory
            .write()
            .await
            .insert(snapshot_index, resource);
        Ok(())
    }

    async fn open_snapshot(&self, x: Index) -> anyhow::Result<lol_core::snapshot::SnapshotStream> {
        let resource = self
            .snapshot_inventory
            .read()
            .await
            .get(&x)
            .unwrap()
            .clone();
        let b: BytesSnapshot = BytesSnapshot {
            contents: bincode::serialize(&resource.0).unwrap().into(),
        };
        let st = b.open_snapshot_stream().await;
        Ok(st)
    }

    async fn delete_snapshot(&self, x: Index) -> anyhow::Result<()> {
        self.snapshot_inventory.write().await.remove(&x);
        Ok(())
    }
}

fn main() {}
