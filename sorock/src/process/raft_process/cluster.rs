use crate::process::raft_process::task::process_configuration_command;

use super::*;

impl RaftProcess {
    /// Forming a new cluster with a single node is called "cluster bootstrapping".
    /// Raft algorith doesn't define adding node when the cluster is empty.
    /// We need to handle this special case.
    async fn bootstrap_cluster(&self) -> Result<()> {
        let mut membership = HashSet::new();
        membership.insert(self.driver.self_node_id());

        let command = Command::serialize(Command::ClusterConfiguration { membership });
        let config = Entry {
            prev_clock: Clock { term: 0, index: 1 },
            this_clock: Clock { term: 0, index: 2 },
            command: command.clone(),
        };
        self.command_log.insert_entry(config).await?;

        process_configuration_command::Task {
            peers: self.peers.clone(),
            voter: Ref(self.voter.clone()),
        }.exec(&command, 2).await?;

        // After this function is called
        // this server should immediately become the leader by self-vote and advance commit index.
        // Consequently, when initial install_snapshot is called this server is already the leader.
        let conn = self.driver.connect(self.driver.self_node_id());
        conn.send_timeout_now().await?;

        Ok(())
    }

    pub(crate) async fn add_server(&self, req: request::AddServer) -> Result<()> {
        if self.peers.read_membership().is_empty() && req.server_id == self.driver.self_node_id() {
            self.bootstrap_cluster().await?;
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
