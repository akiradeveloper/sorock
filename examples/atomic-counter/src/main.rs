use anyhow::Result;
use atomic_counter::{Rep, Req};
use lol_core::simple;
use std::sync::atomic::{AtomicU64, Ordering};

#[derive(serde::Serialize, serde::Deserialize, Clone)]
struct Snapshot(u64);

struct MyApp {
    v: AtomicU64,
}
impl MyApp {
    fn new() -> Self {
        Self {
            v: AtomicU64::new(0),
        }
    }
}
#[tonic::async_trait]
impl simple::RaftAppSimple for MyApp {
    async fn process_read(&self, request: &[u8]) -> Result<Vec<u8>> {
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

    async fn process_write(&self, request: &[u8]) -> Result<(Vec<u8>, Option<Vec<u8>>)> {
        let req = bincode::deserialize(&request)?;
        match req {
            Req::IncAndGet => {
                let old_v = self.v.fetch_add(1, Ordering::SeqCst);
                let rep = Rep::IncAndGet(old_v);
                let rep = Rep::serialize(&rep);
                Ok((rep, None))
            }
            _ => unreachable!(),
        }
    }

    async fn install_snapshot(&self, snapshot: Option<&[u8]>) -> Result<()> {
        match snapshot {
            None => {
                self.v.store(0, Ordering::SeqCst);
            }
            Some(bin) => {
                let snapshot: Snapshot = bincode::deserialize(bin).unwrap();
                self.v.store(snapshot.0, Ordering::SeqCst);
            }
        }
        Ok(())
    }

    async fn fold_snapshot(
        &self,
        old_snapshot: Option<&[u8]>,
        requests: Vec<&[u8]>,
    ) -> Result<Vec<u8>> {
        let mut acc = match old_snapshot {
            None => 0,
            Some(bin) => {
                let snapshot: Snapshot = bincode::deserialize(bin).unwrap();
                snapshot.0
            }
        };
        for req in requests {
            let x = Req::deserialize(&req).unwrap();
            match x {
                Req::IncAndGet => acc += 1,
                _ => unreachable!(),
            }
        }
        let new_snapshot = Snapshot(acc);
        let bin = bincode::serialize(&new_snapshot).unwrap();
        Ok(bin)
    }
}

fn main() {}
