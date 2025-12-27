use super::*;

pub struct Manipulator {
    shard_id: u32,
    io_node: Uri,
    target: ShardState,
}

impl Manipulator {
    pub fn new(shard_id: u32, target: ShardState) -> Self {
        let io_node = target.pick_one_node();
        Self {
            shard_id,
            io_node,
            target,
        }
    }

    async fn get_current_state(&self) -> Result<ShardState> {
        todo!()
    }

    pub async fn run_once(&mut self) -> Result<()> {
        let cur = self.get_current_state().await?;
        self.io_node = cur.pick_one_node();

        let (uri, action) = {
            let mut g = HashSet::new();
            for (uri, _) in &self.target.h {
                g.insert(uri.clone());
            }
            for (uri, _) in &cur.h {
                g.insert(uri.clone());
            }
            let mut m = vec![];
            for uri in g {
                let from = cur.h.get(&uri).cloned().unwrap_or(calc::State {
                    exists: false,
                    is_voter: false,
                    is_leader: false,
                });
                let to = self.target.h.get(&uri).cloned().unwrap_or(calc::State {
                    exists: false,
                    is_voter: false,
                    is_leader: false,
                });
                m.push((uri.clone(), from, to));
            }
            calc::calculate_next_action(m)
        };

        let mut client = RaftClient::connect(self.io_node.clone()).await?;
        match action {
            calc::Action::AddServer => {
                let req = sorock::AddServerRequest {
                    shard_id: self.shard_id,
                    server_id: uri.to_string(),
                    as_voter: false,
                };
                client.add_server(req).await?;
            }
            calc::Action::PromoteToVoter => {
                let req = sorock::AddServerRequest {
                    shard_id: self.shard_id,
                    server_id: uri.to_string(),
                    as_voter: true,
                };
                client.add_server(req).await?;
            }
            calc::Action::NominateLeader => {
                let mut client = RaftClient::connect(uri.clone()).await?;
                let req = sorock::TimeoutNow {
                    shard_id: self.shard_id,
                };
                client.send_timeout_now(req).await?;
            }
            calc::Action::DethroneLeader => {
                // no action
            }
            calc::Action::DemoterToLeaner => {
                let req = sorock::AddServerRequest {
                    shard_id: self.shard_id,
                    server_id: uri.to_string(),
                    as_voter: false,
                };
                client.add_server(req).await?;
            }
            calc::Action::RemoveServer => {
                let req = sorock::RemoveServerRequest {
                    shard_id: self.shard_id,
                    server_id: uri.to_string(),
                };
                client.remove_server(req).await?;
            }
            calc::Action::None => {
                // no action
            }
        }

        Ok(())
    }
}
