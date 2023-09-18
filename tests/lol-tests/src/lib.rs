use anyhow::Result;
use env::Env;
use lol2::client::RaftClient;

pub struct Cluster {
    env: Env,
}
impl Cluster {
    pub async fn new(n: u8) -> Result<Self> {
        let mut env = Env::new()?;
        for id in 0..n {
            env.create(id).await?;
            env.start(id).await?;
            env.connect_network(id).await?;
        }
        Ok(Self { env })
    }
    pub fn raw_env(&mut self) -> &mut Env {
        &mut self.env
    }
    pub fn user(&self, id: u8) -> testapp::Client {
        let conn = self.env.connect(id);
        testapp::Client::new(conn)
    }
    fn admin(&self, id: u8) -> RaftClient {
        let conn = self.env.connect(id);
        lol2::client::RaftClient::new(conn)
    }
    pub async fn add_server(&mut self, to: u8, id: u8) -> Result<()> {
        let mut cli = self.admin(to);
        cli.add_server(lol2::client::AddServerRequest {
            server_id: Env::address_from_id(id),
        })
        .await?;
        Ok(())
    }
    pub async fn remove_server(&mut self, to: u8, id: u8) -> Result<()> {
        let mut cli = self.admin(to);
        cli.remove_server(lol2::client::RemoveServerRequest {
            server_id: Env::address_from_id(id),
        })
        .await?;
        Ok(())
    }
    pub async fn try_commit(&mut self, to: u8) -> Result<()> {
        let mut cli = self.admin(to);
        wait_for_noop_commit(&mut cli).await?;
        Ok(())
    }
}

async fn wait_for_noop_commit(cli: &mut RaftClient) -> Result<(), tonic::Status> {
    use tokio_retry::strategy::{jitter, ExponentialBackoff};
    use tokio_retry::Retry;

    // 200ms, 400, 800, 1600, 3200
    let strategy = ExponentialBackoff::from_millis(2)
        .factor(100)
        .map(jitter)
        .take(5);

    let fut = Retry::spawn(strategy, || {
        let mut cli = cli.clone();
        async move {
            let req = tonic::Request::new(());
            cli.noop(req).await
        }
    });
    fut.await.map(|_| ())
}
