use anyhow::Result;
use tonic::codegen::CompressionEncoding;

mod app;

mod proto {
    tonic::include_proto!("testapp");
}

struct PingApp;

#[tonic::async_trait]
impl proto::ping_server::Ping for PingApp {
    async fn ping(
        &self,
        _: tonic::Request<()>,
    ) -> std::result::Result<tonic::Response<()>, tonic::Status> {
        Ok(tonic::Response::new(()))
    }

    async fn panic(
        &self,
        _: tonic::Request<()>,
    ) -> std::result::Result<tonic::Response<()>, tonic::Status> {
        panic!()
    }
}

#[derive(serde::Deserialize, Debug)]
struct EnvConfig {
    address: String,
    n_lanes: u32,
}

#[tokio::main]
async fn main() -> Result<()> {
    console_subscriber::ConsoleLayer::builder()
        .retention(std::time::Duration::from_secs(60))
        .server_addr(([0, 0, 0, 0], 6669))
        .init();

    env_logger::init();

    let env_config: EnvConfig = envy::from_env()?;
    dbg!(&env_config);

    let rx = {
        use futures::stream::StreamExt;
        use signal_hook::consts::signal::*;
        use signal_hook_tokio::Signals;
        let (tx, rx) = tokio::sync::oneshot::channel();
        let mut signals = Signals::new(&[SIGTERM, SIGINT, SIGQUIT])?;
        tokio::spawn(async move {
            while let Some(signal) = signals.next().await {
                match signal {
                    SIGTERM | SIGINT | SIGQUIT => {
                        tx.send(()).ok();
                        break;
                    }
                    _ => unreachable!(),
                }
            }
        });
        rx
    };

    let node_id = env_config.address.parse()?;
    let node = lolraft::RaftNode::new(node_id);

    for lane_id in 0..env_config.n_lanes {
        let driver = node.get_driver(lane_id);
        let process = app::new(driver).await?;
        node.attach_process(lane_id, process);
    }

    let raft_svc = lolraft::raft_service::new(node)
        .send_compressed(CompressionEncoding::Zstd)
        .accept_compressed(CompressionEncoding::Zstd);
    let reflection_svc = lolraft::reflection_service::new();
    let ping_svc = proto::ping_server::PingServer::new(PingApp);
    let socket = format!("0.0.0.0:50000").parse()?;

    let mut builder = tonic::transport::Server::builder();
    builder
        .add_service(raft_svc)
        .add_service(reflection_svc)
        .add_service(ping_svc)
        .serve_with_shutdown(socket, async {
            rx.await.ok();
        })
        .await?;

    Ok(())
}
