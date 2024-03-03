use super::*;

impl RaftProcess {
    /// Process configuration change if the command contains configuration.
    /// Configuration should be applied as soon as it is inserted into the log because doing so
    /// guarantees that majority of the servers move to the configuration when the entry is committed.
    /// Without this property, servers may still be in some old configuration which may cause split-brain
    /// by electing two leaders in a single term which is not allowed in Raft.
    pub(crate) async fn process_configuration_command(
        &self,
        command: &[u8],
        index: Index,
    ) -> Result<()> {
        let config0 = match Command::deserialize(command) {
            Command::Snapshot { membership } => Some(membership),
            Command::ClusterConfiguration { membership } => Some(membership),
            _ => None,
        };
        if let Some(config) = config0 {
            self.peers
                .set_membership(config, index, Ref(self.voter.clone()))
                .await?;
        }
        Ok(())
    }

    async fn init_cluster(&self) -> Result<()> {
        let mut membership = HashSet::new();
        membership.insert(self.driver.self_node_id());

        let init_command = Command::serialize(Command::Snapshot {
            membership: membership.clone(),
        });
        let snapshot = Entry {
            prev_clock: Clock { term: 0, index: 0 },
            this_clock: Clock { term: 0, index: 1 },
            command: init_command.clone(),
        };

        self.command_log.insert_snapshot(snapshot).await?;
        self.process_configuration_command(&init_command, 1).await?;

        // After this function is called
        // this server immediately becomes the leader by self-vote and advance commit index.
        // Consequently, when initial install_snapshot is called this server is already the leader.
        let conn = self.driver.connect(self.driver.self_node_id());
        conn.send_timeout_now().await?;

        Ok(())
    }

    pub(crate) async fn add_server(&self, req: request::AddServer) -> Result<()> {
        if self.peers.read_membership().is_empty() && req.server_id == self.driver.self_node_id() {
            // This is called the "cluster bootstrapping".
            // To add a node to a cluster we have to know some node in the cluster.
            // But what about the first node?
            self.init_cluster().await?;
        } else {
            let msg = kern_message::KernRequest::AddServer(req.server_id);
            let req = request::KernRequest {
                message: msg.serialize(),
            };
            let conn = self.driver.connect(self.driver.self_node_id());
            conn.process_kern_request(req).await?;
        }
        Ok(())
    }

    pub(crate) async fn remove_server(&self, req: request::RemoveServer) -> Result<()> {
        let msg = kern_message::KernRequest::RemoveServer(req.server_id);
        let req = request::KernRequest {
            message: msg.serialize(),
        };
        let conn = self.driver.connect(self.driver.self_node_id());
        conn.process_kern_request(req).await?;
        Ok(())
    }
}
