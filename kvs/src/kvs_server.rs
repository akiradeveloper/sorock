use anyhow::anyhow;
use async_trait::async_trait;
use lol_core::connection;
use lol_core::{Index, Config, Message, RaftApp, RaftCore, TunableConfig, SnapshotTag, storage::RaftStorage};
use lol_core::snapshot::{SnapshotStream, BytesSnapshot};
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;
use structopt::StructOpt;
use tokio::sync::RwLock;
use std::path::Path;

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
struct KVS {
    mem: Arc<RwLock<BTreeMap<String, String>>>,
    copy_snapshot_mode: bool,
}
#[async_trait]
impl RaftApp for KVS {
    async fn process_message(&self, x: Message) -> anyhow::Result<Message> {
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
    async fn apply_message(&self, x: Message, _: Index) -> anyhow::Result<(Message, Option<SnapshotTag>)> {
        let res = self.process_message(x).await?;
        let new_snapshot = if self.copy_snapshot_mode {
            let new_snapshot = kvs::Snapshot { h: self.mem.read().await.clone() };
            let b = kvs::Snapshot::serialize(&new_snapshot);
            Some(b.into())
        } else {
            None
        };
        Ok((res, new_snapshot))
    }
    async fn install_snapshot(&self, x: Option<&SnapshotTag>, _: Index) -> anyhow::Result<()> {
        if let Some(x) = x {
            // emulate heavy install_snapshot
            tokio::time::delay_for(Duration::from_secs(10)).await;
            let mut h = self.mem.write().await;
            let snapshot = kvs::Snapshot::deserialize(x.as_ref()).unwrap();
            h.clear();
            for (k, v) in snapshot.h {
                h.insert(k, v);
            }
        }
        Ok(())
    }
    async fn fold_snapshot(
        &self,
        old_snapshot: Option<&SnapshotTag>,
        xs: Vec<Message>,
    ) -> anyhow::Result<SnapshotTag> {
        let mut old = old_snapshot
            .map(|x| kvs::Snapshot::deserialize(x.as_ref()).unwrap())
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
        let b = kvs::Snapshot::serialize(&old);
        Ok(b.into())
    }
    async fn from_snapshot_stream(&self, st: SnapshotStream) -> anyhow::Result<SnapshotTag> {
        let b = BytesSnapshot::from_snapshot_stream(st).await?;
        let b = b.as_ref().to_vec();
        Ok(b.into())
    }
    async fn to_snapshot_stream(&self, x: &SnapshotTag) -> SnapshotStream {
        let b: BytesSnapshot = x.as_ref().to_vec().into();
        b.to_snapshot_stream().await
    }
    async fn delete_resource(&self, _: &SnapshotTag) -> anyhow::Result<()> {
        Ok(())
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

    let id = connection::resolve(&opt.id).unwrap();
    let config = Config { id: id.clone() };
    let mut tunable = TunableConfig::default();

    if !opt.copy_snapshot_mode {
        // by default, compactions runs every 5 secs.
        // this value is carefully chosen, the tests depends on this value.
        let v = opt.compaction_interval_sec.unwrap_or(5);
        tunable.compaction_interval_sec = v;
        tunable.compaction_delay_sec = v;
    } else {
        tunable.compaction_interval_sec = 0;
        tunable.compaction_delay_sec = 1;
    }

    let id_cln = id.clone();
    env_logger::builder()
        .format(move |buf, record| {
            let ts = buf.timestamp();
            writeln!(buf, "[{} {}] {}> {}", ts, record.level(), id_cln, record.args())
        })
        .init();

    let core = if let Some(id) = opt.use_persistency {
        std::fs::create_dir("/tmp/lol");
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

    let res = lol_core::start_server(core).await;
    if res.is_err() {
        eprintln!("failed to start kvs-server error={:?}", res);
    }
}
