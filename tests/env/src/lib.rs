use anyhow::Result;
use core::error;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Once;
use std::time::Duration;
use tempfile::NamedTempFile;
use tonic::codegen::CompressionEncoding;
use tonic::transport::{Channel, Endpoint, Uri};
use tracing::{error, info};

static INIT: Once = Once::new();

struct Node {
    port: u16,
    abort_tx0: Option<tokio::sync::oneshot::Sender<()>>,
}
impl Node {
    pub fn new(
        id: u8,
        port: u16,
        n_shards: u32,
        pstate: Option<Arc<PersistentState>>,
    ) -> Result<Self> {
        let nd_tag = format!("ND{port}>");
        let (tx, rx) = tokio::sync::oneshot::channel();

        let svc_task = async move {
            info!("env: add (id={id})");

            let node_id = {
                let address = format!("http://127.0.0.1:{port}");
                address.parse().unwrap()
            };
            let node = sorock::RaftNode::new(node_id);

            info!("env: create db");
            let db = {
                let db = match &pstate {
                    Some(file) => match redb::Database::create(file.log_file.path()) {
                        Ok(x) => x,
                        Err(e) => {
                            error!("failed to create db: {:?}", e);
                            panic!()
                        }
                    },
                    None => {
                        let mem = redb::backends::InMemoryBackend::new();
                        redb::Database::builder().create_with_backend(mem).unwrap()
                    }
                };
                sorock::backend::redb::Backend::new(db)
            };
            info!("env: db created");

            for shard_id in 0..n_shards {
                let state = pstate
                    .as_ref()
                    .map(|env| env.snapshot_files[shard_id as usize].path());
                let (log, ballot) = db.get(shard_id).unwrap();
                let driver = node.get_driver(shard_id);
                let process = testapp::raft_process::new(state, log, ballot, driver)
                    .await
                    .unwrap();
                node.attach_process(shard_id, process);
            }

            let raft_svc = sorock::service::raft::new(node.clone())
                .send_compressed(CompressionEncoding::Zstd)
                .accept_compressed(CompressionEncoding::Zstd);
            let monitor_svc = sorock::service::monitor::new(node);
            let reflection_svc = sorock::service::reflection::new();
            let ping_svc = testapp::ping_app::new_service();

            let socket = format!("127.0.0.1:{port}").parse().unwrap();

            let mut builder = tonic::transport::Server::builder();
            builder
                .add_service(raft_svc)
                .add_service(monitor_svc)
                .add_service(reflection_svc)
                .add_service(ping_svc)
                .serve_with_shutdown(socket, async {
                    info!("env: remove (id={id})");
                    rx.await.ok();
                })
                .await
                .unwrap();
        };

        std::thread::Builder::new()
            .name(nd_tag.clone())
            .spawn(move || {
                let runtime = tokio::runtime::Builder::new_multi_thread()
                    .thread_name(nd_tag)
                    .enable_all()
                    .build()
                    .unwrap();
                runtime.block_on(svc_task);
            })
            .unwrap();

        Ok(Self {
            port,
            abort_tx0: Some(tx),
        })
    }

    fn address(&self) -> Uri {
        let uri = format!("http://127.0.0.1:{}", self.port);
        Uri::from_maybe_shared(uri).unwrap()
    }
}
impl Drop for Node {
    fn drop(&mut self) {
        let tx = self.abort_tx0.take().unwrap();
        tx.send(()).ok();
    }
}

struct PersistentState {
    log_file: NamedTempFile,
    snapshot_files: Vec<NamedTempFile>,
}
impl PersistentState {
    fn new(n_shards: u32) -> Self {
        let log_file = NamedTempFile::new().unwrap();
        let mut snapshot_files = vec![];
        for _ in 0..n_shards {
            let snapshot_file = NamedTempFile::new().unwrap();
            snapshot_files.push(snapshot_file);
        }
        Self {
            log_file,
            snapshot_files,
        }
    }
}

struct PersistentEnv {
    files: HashMap<u8, Arc<PersistentState>>,
    n_shards: u32,
}
impl PersistentEnv {
    fn new(n_shards: u32) -> Self {
        Self {
            files: HashMap::new(),
            n_shards,
        }
    }
    fn get(&mut self, id: u8) -> Arc<PersistentState> {
        self.files
            .entry(id)
            .or_insert_with(|| Arc::new(PersistentState::new(self.n_shards)))
            .clone()
    }
}

pub struct Env {
    n_shards: u32,
    allocated_ports: HashMap<u8, u16>,
    nodes: HashMap<u8, Node>,
    conn_cache: spin::Mutex<HashMap<u8, Channel>>,
    penv: Option<PersistentEnv>,
}
impl Env {
    pub fn new(n_shards: u32, with_persistency: bool, with_logging: bool) -> Self {
        INIT.call_once(|| {
            // On terminating the tokio runtime,
            // flooding stack traces are printed and they are super noisy.
            // Until better idea is invented, we just suppress them.
            std::panic::set_hook(Box::new(|_info| {}));

            if with_logging {
                let format = tracing_subscriber::fmt::format()
                    .with_target(false)
                    .with_thread_names(true)
                    .compact();
                tracing_subscriber::fmt().event_format(format).init();
            }
        });
        let penv = if with_persistency {
            Some(PersistentEnv::new(n_shards))
        } else {
            None
        };
        Self {
            n_shards,
            nodes: HashMap::new(),
            allocated_ports: HashMap::new(),
            conn_cache: spin::Mutex::new(HashMap::new()),
            penv,
        }
    }

    pub fn add_node(&mut self, id: u8) {
        let free_port = *self
            .allocated_ports
            .entry(id)
            .or_insert_with(|| port_check::free_local_ipv4_port().unwrap());
        let snap_states = self.penv.as_mut().map(|env| env.get(id));
        let node = Node::new(id, free_port, self.n_shards, snap_states).unwrap();
        port_check::is_port_reachable_with_timeout(
            node.address().to_string(),
            Duration::from_secs(5),
        );
        self.nodes.insert(id, node);
    }

    pub fn remove_node(&mut self, id: u8) {
        if let Some(_node) = self.nodes.remove(&id) {
            // node is dropped
        }
    }

    pub fn get_connection(&self, id: u8) -> Channel {
        self.conn_cache
            .lock()
            .entry(id)
            .or_insert_with(|| {
                let uri = self.nodes.get(&id).unwrap().address();
                let endpoint = Endpoint::from(uri)
                    .http2_keep_alive_interval(std::time::Duration::from_secs(1))
                    .keep_alive_while_idle(true)
                    .timeout(std::time::Duration::from_secs(5))
                    .connect_timeout(std::time::Duration::from_secs(5));
                let chan = endpoint.connect_lazy();
                chan
            })
            .clone()
    }

    pub async fn connect_ping_client(
        &self,
        id: u8,
    ) -> Result<testapp::ping_app::PingClient<Channel>> {
        let uri = self.nodes.get(&id).unwrap().address();
        let endpoint = Endpoint::from(uri)
            .timeout(std::time::Duration::from_secs(1))
            .connect_timeout(std::time::Duration::from_secs(1));
        let chan = endpoint.connect().await?;
        let cli = testapp::ping_app::PingClient::new(chan);
        Ok(cli)
    }

    pub fn address(&self, id: u8) -> Uri {
        self.nodes.get(&id).unwrap().address()
    }

    pub async fn check_connectivity(&self, id: u8) -> Result<()> {
        for _ in 0..50 {
            let uri = self.nodes.get(&id).unwrap().address();
            let endpoint =
                Endpoint::from(uri).connect_timeout(std::time::Duration::from_millis(100));
            match endpoint.connect().await {
                Ok(_) => return Ok(()),
                Err(_) => {
                    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                }
            }
        }
        anyhow::bail!("failed to connect to id={}", id);
    }
}
