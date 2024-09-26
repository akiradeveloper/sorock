use anyhow::{ensure, Result};
use env::Env;
use lolraft::client::RaftClient;

pub struct Cluster {
    env: Env,
}
impl Cluster {
    /// Create `n` nodes and connect them to a network.
    pub async fn new(n: u8, n_shards: u32) -> Result<Self> {
        ensure!(n > 0);
        ensure!(n_shards > 0);
        let mut env = Env::new();
        for id in 0..n {
            env.add_node(id, n_shards);
            env.check_connectivity(0).await?;
        }
        Ok(Self { env })
    }

    pub fn env(&mut self) -> &mut Env {
        &mut self.env
    }

    /// Get an application client to connect to node `id`.
    pub fn user(&self, id: u8) -> testapp::Client {
        let conn = self.env.get_connection(id);
        testapp::Client::new(conn)
    }

    pub fn admin(&self, id: u8) -> RaftClient {
        let conn = self.env.get_connection(id);
        lolraft::client::RaftClient::new(conn)
    }

    /// Request node `to` to add a node `id`.
    pub async fn add_server(&self, shard_id: u32, to: u8, id: u8) -> Result<()> {
        self.admin(to)
            .add_server(lolraft::client::AddServerRequest {
                shard_id,
                server_id: self.env.address(id).to_string(),
            })
            .await?;
        // Make sure the newly added server knows the current leader.
        self.user(id).fetch_add(shard_id, 0).await?;
        Ok(())
    }

    /// Request node `to` to remove a node `id`.
    pub async fn remove_server(&self, shard_id: u32, to: u8, id: u8) -> Result<()> {
        self.admin(to)
            .remove_server(lolraft::client::RemoveServerRequest {
                shard_id,
                server_id: self.env.address(id).to_string(),
            })
            .await?;
        eprintln!("removed node(id={id})");
        // Make sure consensus can be made after removing the server.
        self.user(to).fetch_add(shard_id, 0).await?;
        Ok(())
    }
}
