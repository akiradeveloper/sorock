use anyhow::anyhow;
use async_trait::async_trait;
use lol_core::connection;
use lol_core::{Config, Message, RaftApp, RaftCore, Snapshot, TunableConfig};
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;
use structopt::StructOpt;
use tokio::sync::RwLock;

#[derive(StructOpt, Debug)]
#[structopt(name = "kvs-server")]
struct Opt {
    #[structopt(name = "ID")]
    id: String,
}
struct KVS {
    mem: Arc<RwLock<BTreeMap<String, String>>>,
}
#[async_trait]
impl RaftApp for KVS {
    async fn apply_message(&self, x: Message) -> anyhow::Result<Message> {
        let msg = kvs::Req::deserialize(&x);
        match msg {
            Some(x) => match x {
                kvs::Req::Set { key, value } => {
                    let mut writer = self.mem.write().await;
                    writer.insert(key, value);
                    let res = kvs::Rep::Set {};
                    Ok(kvs::Rep::serialize(&res))
                }
                kvs::Req::Get { key } => {
                    let v = self.mem.read().await.get(&key).cloned();
                    let (found, value) = match v {
                        Some(x) => (true, x),
                        None => (false, "".to_owned()),
                    };
                    let res = kvs::Rep::Get { found, value };
                    Ok(kvs::Rep::serialize(&res))
                }
                kvs::Req::List => {
                    let values = self.mem.read().await.clone().into_iter().collect();
                    let res = kvs::Rep::List { values };
                    Ok(kvs::Rep::serialize(&res))
                }
            },
            None => Err(anyhow!("the message not supported")),
        }
    }
    async fn install_snapshot(&self, x: Snapshot) -> anyhow::Result<()> {
        if let Some(x) = x {
            // emulate heavy install_snapshot
            tokio::time::delay_for(Duration::from_secs(10)).await;
            let mut h = self.mem.write().await;
            let snapshot = kvs::Snapshot::deserialize(&x).unwrap();
            h.clear();
            for (k, v) in snapshot.h {
                h.insert(k, v);
            }
        }
        Ok(())
    }
    async fn fold_snapshot(
        &self,
        old_snapshot: Snapshot,
        xs: Vec<Message>,
    ) -> anyhow::Result<Snapshot> {
        let mut old = old_snapshot
            .map(|x| kvs::Snapshot::deserialize(&x).unwrap())
            .unwrap_or(kvs::Snapshot { h: BTreeMap::new() });

        for x in xs {
            let req = kvs::Req::deserialize(&x);
            match req {
                Some(x) => match x {
                    kvs::Req::Set { key, value } => {
                        old.h.insert(key, value);
                    }
                    _ => {}
                },
                None => {}
            }
        }

        Ok(Some(kvs::Snapshot::serialize(&old)))
    }
}
impl KVS {
    pub fn new() -> Self {
        Self {
            mem: Arc::new(RwLock::new(BTreeMap::new())),
        }
    }
}

use std::io::Write;

#[tokio::main]
async fn main() {
    let opt = Opt::from_args();
    let app = KVS::new();
    let id = connection::resolve(&opt.id).unwrap();
    let config = Config { id: id.clone() };
    let mut tunable = TunableConfig::new();

    // compactions runs every 5 secs.
    // be careful, the tests depends on this value.
    tunable.compaction_interval_sec = 5;

    env_logger::builder()
        .format(move |buf, record| {
            let ts = buf.timestamp();
            writeln!(buf, "[{} {}] {}> {}", ts, record.level(), id, record.args())
        })
        .init();

    let storage = lol_core::storage::memory::Storage::new();
    let core = RaftCore::new(app, storage, config, tunable).await;
    let core = Arc::new(core);
    let res = lol_core::start_server(core).await;
    if res.is_err() {
        eprintln!("failed to start kvs-server error={:?}", res);
    }
}
