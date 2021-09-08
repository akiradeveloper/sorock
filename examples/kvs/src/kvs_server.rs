use anyhow::anyhow;
use async_trait::async_trait;
use bytes::Bytes;
use lol_core::compat::{RaftAppCompat, ToRaftApp};
use lol_core::{Config, Index, RaftCore, TunableConfig};
use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;
use structopt::StructOpt;
use tokio::sync::oneshot;
use tokio::sync::RwLock;
use kvs::{Req, Rep};

#[derive(StructOpt, Debug)]
#[structopt(name = "kvs-server")]
struct Opt {
    // use persistent storage with is identified by the given id.
    #[structopt(long)]
    use_persistency: Option<u8>,
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
impl RaftAppCompat for KVS {
    async fn process_message(&self, x: &[u8]) -> anyhow::Result<Vec<u8>> {
        let msg = Req::deserialize(&x);
        match msg {
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
            },
            None => Err(anyhow!("the message not supported")),
        }
    }
    async fn apply_message(
        &self,
        x: &[u8],
        _: Index,
    ) -> anyhow::Result<(Vec<u8>, Option<Vec<u8>>)> {
        let res = self.process_message(x).await?;
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
    async fn install_snapshot(&self, x: Option<&[u8]>, _: Index) -> anyhow::Result<()> {
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
    let url = url::Url::parse(&id).unwrap();
    let host_port_str = format!("{}:{}", url.host_str().unwrap(), url.port().unwrap());
    let socket = tokio::net::lookup_host(host_port_str)
        .await
        .unwrap()
        .next()
        .unwrap();

    let config = Config::new(id.clone());
    let mut tunable = TunableConfig::default();

    if !opt.copy_snapshot_mode {
        // by default, compactions runs every 5 secs.
        // this value is carefully chosen, the tests depends on this value.
        let v = opt.compaction_interval_sec.unwrap_or(5);
        tunable.compaction_interval_sec = v;
        tunable.compaction_delay_sec = 1;
    } else {
        tunable.compaction_interval_sec = 0;
        tunable.compaction_delay_sec = 1;
    }

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

    let app = ToRaftApp::new(app);
    let core = if let Some(id) = opt.use_persistency {
        std::fs::create_dir("/tmp/lol").ok();
        let path = format!("/tmp/lol/{}.db", id);
        let path = Path::new(&path);
        let builder = lol_core::storage::disk::StorageBuilder::new(&path);
        if opt.reset_persistency {
            builder.destory();
            builder.create();
        }
        let storage = builder.open();
        RaftCore::new(app, storage, config, tunable).await
    } else {
        let storage = lol_core::storage::memory::Storage::new();
        RaftCore::new(app, storage, config, tunable).await
    };

    let service = lol_core::make_service(core);
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
