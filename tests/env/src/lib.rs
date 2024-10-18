use anyhow::Result;
use std::collections::HashMap;
use std::sync::Once;
use std::time::Duration;
use tonic::codegen::CompressionEncoding;
use tonic::transport::{Channel, Endpoint, Uri};
use tracing::info;

static INIT: Once = Once::new();

struct Node {
    port: u16,
    abort_tx0: Option<tokio::sync::oneshot::Sender<()>>,
}
impl Node {
    pub fn new(id: u8, port: u16, n_shards: u32) -> Result<Self> {
        let nd_tag = format!("ND{port}>");
        let (tx, rx) = tokio::sync::oneshot::channel();

        let svc_task = async move {
            info!("add (id={id})");

            let node_id = {
                let address = format!("http://127.0.0.1:{port}");
                address.parse().unwrap()
            };
            let node = sorock::RaftNode::new(node_id);

            let db = {
                let mem = redb::backends::InMemoryBackend::new();
                let db = redb::Database::builder().create_with_backend(mem).unwrap();
                sorock::backends::redb::Backend::new(db)
            };

            for shard_id in 0..n_shards {
                let (log, ballot) = db.get(shard_id).unwrap();
                let driver = node.get_driver(shard_id);
                let process = testapp::raft_process::new(id, log, ballot, driver)
                    .await
                    .unwrap();
                node.attach_process(shard_id, process);
            }

            let raft_svc = sorock::raft_service::new(node.clone())
                .send_compressed(CompressionEncoding::Zstd)
                .accept_compressed(CompressionEncoding::Zstd);
            let monitor_svc = sorock::monitor_service::new(node);
            let reflection_svc = sorock::reflection_service::new();
            let ping_svc = testapp::ping_app::new_service();

            let socket = format!("127.0.0.1:{port}").parse().unwrap();

            let mut builder = tonic::transport::Server::builder();
            builder
                .add_service(raft_svc)
                .add_service(monitor_svc)
                .add_service(reflection_svc)
                .add_service(ping_svc)
                .serve_with_shutdown(socket, async {
                    info!("remove (id={id})");
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
pub struct Env {
    nodes: HashMap<u8, Node>,
    conn_cache: spin::Mutex<HashMap<u8, Channel>>,
}
impl Env {
    pub fn new(with_logging: bool) -> Self {
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
        Self {
            nodes: HashMap::new(),
            conn_cache: spin::Mutex::new(HashMap::new()),
        }
    }

    pub fn add_node(&mut self, id: u8, n_shards: u32) {
        let free_port = port_check::free_local_ipv4_port().unwrap();
        let node = Node::new(id, free_port, n_shards).unwrap();
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
                dbg!(id, &uri);
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
