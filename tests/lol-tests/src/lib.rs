use anyhow::Result;
use env::Env;
use lolraft::client::RaftClient;

pub struct Cluster {
    env: Env,
}
impl Cluster {
    /// Create `n` nodes and connect them to a network.
    pub async fn new(n: u8) -> Result<Self> {
        let mut env = Env::new()?;
        for id in 0..n {
            env.create(id).await?;
            env.start(id).await?;
            env.connect_network(id).await?;
        }
        Ok(Self { env })
    }

    pub fn env(&mut self) -> &mut Env {
        &mut self.env
    }

    /// Get an application client to connect to node `id`.
    pub fn user(&self, id: u8) -> testapp::Client {
        let conn = self.env.connect(id);
        testapp::Client::new(conn)
    }

    fn admin(&self, id: u8) -> RaftClient {
        let conn = self.env.connect(id);
        lolraft::client::RaftClient::new(conn)
    }

    /// Request node `to` to add a node `id`.
    pub async fn add_server(&mut self, to: u8, id: u8) -> Result<()> {
        self.admin(to)
            .add_server(lolraft::client::AddServerRequest {
                lane_id: testapp::APP_LANE_ID,
                server_id: env::address_from_id(id),
            })
            .await?;
        // Make sure the newly added server knows the current leader.
        self.user(id).fetch_add(0).await?;
        Ok(())
    }

    /// Request node `to` to remove a node `id`.
    pub async fn remove_server(&mut self, to: u8, id: u8) -> Result<()> {
        self.admin(to)
            .remove_server(lolraft::client::RemoveServerRequest {
                lane_id: testapp::APP_LANE_ID,
                server_id: env::address_from_id(id),
            })
            .await?;
        eprintln!("removed node(id={id})");
        // Make sure consensus can be made after removing the server.
        self.user(to).fetch_add(0).await?;
        Ok(())
    }
}
