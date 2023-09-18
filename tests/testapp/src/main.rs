use anyhow::Result;

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
}

#[derive(serde::Deserialize, Debug)]
struct EnvConfig {
    address: String,
}

#[tokio::main]
async fn main() -> Result<()> {
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
    let node = lol2::RaftNode::new(node_id);

    let driver = node.get_driver();
    let process = app::new(driver).await?;
    node.attach_process(process);

    let raft_svc = lol2::raft_service::new(node);
    let ping_svc = proto::ping_server::PingServer::new(PingApp);
    let socket = format!("0.0.0.0:50000").parse()?;

    let mut builder = tonic::transport::Server::builder();
    builder
        .add_service(raft_svc)
        .add_service(ping_svc)
        .serve_with_shutdown(socket, async {
            rx.await.ok();
        })
        .await?;

    Ok(())
}
