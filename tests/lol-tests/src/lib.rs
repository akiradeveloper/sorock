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
        self.admin(to)
            .add_server(lol2::client::AddServerRequest {
                lane_id: testapp::APP_LANE_ID,
                server_id: env::address_from_id(id),
            })
            .await?;
        // make sure the new server is aquiainted with the current leader.
        self.user(id).fetch_add(0).await?;
        Ok(())
    }

    pub async fn remove_server(&mut self, to: u8, id: u8) -> Result<()> {
        self.admin(to)
            .remove_server(lol2::client::RemoveServerRequest {
                lane_id: testapp::APP_LANE_ID,
                server_id: env::address_from_id(id),
            })
            .await?;
        eprintln!("removed");
        self.user(to).fetch_add(0).await?;
        Ok(())
    }
}
