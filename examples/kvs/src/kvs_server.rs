use anyhow::anyhow;
use async_trait::async_trait;
use bytes::Bytes;
use kvs::{Rep, Req};
use lol_core::simple::{RaftAppSimple, ToRaftApp};
use lol_core::{simple, Config, Index, Uri};
use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;
use structopt::StructOpt;
use tokio::sync::oneshot;
use tokio::sync::RwLock;

const USE_FILE_BACKEND: u8 = 0;
const USE_ROCKSDB_BACKEND: u8 = 1;

#[derive(StructOpt, Debug)]
#[structopt(name = "kvs-server")]
struct Opt {
    // use persistent storage with is identified by the given id.
    #[structopt(long)]
    use_persistency: Option<u8>,
    #[structopt(long, default_value = "0")]
    persistency_backend: u8,
    // if this is set, the persistent storage is reset.
    #[structopt(long)]
    reset_persistency: bool,
    // if copy_snapshot_mode is chosen, fold snapshot is disabled and
    // copy snapshot is made on every apply.
    #[structopt(long)]
    copy_snapshot_mode: bool,
    // the interval of the fold snapshot
    #[structopt(long)]
    compaction_interval_sec: Option<u64>,
    #[structopt(name = "ID")]
    id: String,
}
#[derive(serde::Serialize, serde::Deserialize, std::fmt::Debug)]
pub struct Snapshot {
    pub h: BTreeMap<String, Bytes>,
}
impl Snapshot {
    pub fn serialize(msg: &Snapshot) -> Vec<u8> {
        bincode::serialize(msg).unwrap()
    }
    pub fn deserialize(b: &[u8]) -> Option<Snapshot> {
        bincode::deserialize(b).ok()
    }
}
struct KVS {
    mem: Arc<RwLock<BTreeMap<String, Bytes>>>,
    copy_snapshot_mode: bool,
}
#[async_trait]
impl RaftAppSimple for KVS {
    async fn process_read(&self, x: &[u8]) -> anyhow::Result<Vec<u8>> {
        let msg = Req::deserialize(&x);
        match msg {
            Some(x) => match x {
                Req::Get { key } => {
                    let v = self.mem.read().await.get(&key).cloned();
                    let (found, value) = match v {
                        Some(x) => (true, String::from_utf8(x.as_ref().to_vec()).unwrap()),
                        None => (false, "".to_owned()),
                    };
                    let res = Rep::Get { found, value };
                    Ok(Rep::serialize(&res))
                }
                Req::List => {
                    let mut values = vec![];
                    for (k, v) in self.mem.read().await.iter() {
                        values.push((k.clone(), String::from_utf8(v.as_ref().to_vec()).unwrap()));
                    }
                    let res = Rep::List { values };
                    Ok(Rep::serialize(&res))
                }
                _ => unreachable!(),
            },
            None => Err(anyhow!("the message not supported")),
        }
    }
    async fn process_write(&self, x: &[u8]) -> anyhow::Result<(Vec<u8>, Option<Vec<u8>>)> {
        let msg = Req::deserialize(&x);
        let res = match msg {
            Some(x) => match x {
                Req::Set { key, value } => {
                    let mut writer = self.mem.write().await;
                    writer.insert(key, Bytes::from(value));
                    let res = Rep::Set {};
                    Ok(Rep::serialize(&res))
                }
                Req::SetBytes { key, value } => {
                    let mut writer = self.mem.write().await;
                    writer.insert(key, value);
                    let res = Rep::Set {};
                    Ok(Rep::serialize(&res))
                }
                _ => unreachable!(),
            },
            None => Err(anyhow!("the message not supported")),
        }?;
        let new_snapshot = if self.copy_snapshot_mode {
            let new_snapshot = Snapshot {
                h: self.mem.read().await.clone(),
            };
            let b = Snapshot::serialize(&new_snapshot);
            Some(b)
        } else {
            None
        };
        Ok((res, new_snapshot))
    }
    async fn install_snapshot(&self, x: Option<&[u8]>) -> anyhow::Result<()> {
        if let Some(x) = x {
            let mut h = self.mem.write().await;
            let snapshot = Snapshot::deserialize(x.as_ref()).unwrap();
            h.clear();
            for (k, v) in snapshot.h {
                h.insert(k, v);
            }
        }
        Ok(())
    }
    async fn fold_snapshot(
        &self,
        old_snapshot: Option<&[u8]>,
        xs: Vec<&[u8]>,
    ) -> anyhow::Result<Vec<u8>> {
        let mut old = old_snapshot
            .map(|x| Snapshot::deserialize(x.as_ref()).unwrap())
            .unwrap_or(Snapshot { h: BTreeMap::new() });

        for x in xs {
            let req = Req::deserialize(&x);
            match req {
                Some(x) => match x {
                    Req::Set { key, value } => {
                        old.h.insert(key, Bytes::from(value));
                    }
                    Req::SetBytes { key, value } => {
                        old.h.insert(key, value);
                    }
                    _ => {}
                },
                None => {}
            }
        }
        let b = Snapshot::serialize(&old);
        Ok(b)
    }
}
impl KVS {
    pub fn new() -> Self {
        Self {
            mem: Arc::new(RwLock::new(BTreeMap::new())),
            copy_snapshot_mode: false,
        }
    }
}

use std::io::Write;

#[tokio::main]
async fn main() {
    let opt = Opt::from_args();
    let mut app = KVS::new();
    app.copy_snapshot_mode = opt.copy_snapshot_mode;

    let id = opt.id.clone();
    let id = lol_core::Uri::from_maybe_shared(id).unwrap();
    let host_port_str = format!("{}:{}", id.host().unwrap(), id.port_u16().unwrap());
    let socket = tokio::net::lookup_host(host_port_str)
        .await
        .unwrap()
        .next()
        .unwrap();

    let mut config = lol_core::ConfigBuilder::default();
    if !opt.copy_snapshot_mode {
        // by default, compactions runs every 5 secs.
        // this value is carefully chosen, the tests depends on this value.
        let v = opt.compaction_interval_sec.unwrap_or(5);
        config.compaction_interval_sec(v);
    } else {
        config.compaction_interval_sec(0);
    }
    let config = config.build().unwrap();

    let id_cln = id.clone();
    env_logger::builder()
        .format(move |buf, record| {
            let ts = buf.timestamp();
            writeln!(
                buf,
                "[{} {}] {}> {}",
                ts,
                record.level(),
                id_cln,
                record.args()
            )
        })
        .init();

    if let Some(storage_id) = opt.use_persistency {
        let root_dir = format!("/tmp/lol/{}", storage_id);
        std::fs::create_dir_all(&root_dir).ok();
    }
    let app = if let Some(storage_id) = opt.use_persistency {
        let root_dir = format!("/tmp/lol/{}/snapshots", storage_id);
        let root_dir = Path::new(&root_dir);
        if opt.reset_persistency {
            simple::FileInventory::destroy(&root_dir).unwrap();
            simple::FileInventory::create(&root_dir).unwrap();
        }
        ToRaftApp::new(app, simple::FileInventory::new(root_dir))
    } else {
        ToRaftApp::new(app, simple::BytesInventory::new())
    };
    let service = if let Some(storage_id) = opt.use_persistency {
        let path = format!("/tmp/lol/{}/store", storage_id);
        let path = Path::new(&path);
        match opt.persistency_backend {
            USE_FILE_BACKEND => {
                use lol_core::storage::file::Storage;
                if opt.reset_persistency {
                    Storage::destory(&path).unwrap();
                    Storage::create(&path).unwrap();
                }
                let storage = Storage::open(&path);
                lol_core::make_raft_service(app, storage, id, config).await
            }
            USE_ROCKSDB_BACKEND => {
                use lol_core::storage::rocksdb::Storage;
                if opt.reset_persistency {
                    Storage::destroy(&path);
                    Storage::create(&path);
                }
                let storage = Storage::open(&path);
                lol_core::make_raft_service(app, storage, id, config).await
            }
            _ => unreachable!(),
        }
    } else {
        let storage = lol_core::storage::memory::Storage::new();
        lol_core::make_raft_service(app, storage, id, config).await
    };

    let mut builder = tonic::transport::Server::builder();

    let (tx, rx) = oneshot::channel();
    let _ = tokio::spawn(wait_for_signal(tx));
    let res = builder
        .add_service(service)
        .serve_with_shutdown(socket, async {
            rx.await.ok();
            log::info!("graceful context shutdown");
        })
        .await;
    if res.is_err() {
        log::error!("failed to start kvs-server error={:?}", res);
    }
}
pub async fn wait_for_signal(tx: oneshot::Sender<()>) -> anyhow::Result<()> {
    use tokio::signal::unix;
    let mut stream = unix::signal(unix::SignalKind::interrupt())?;
    stream.recv().await;
    log::info!("SIGINT received: shutting down");
    let _ = tx.send(());
    Ok(())
}
