use super::*;

impl RaftProcess {
    async fn init_cluster(&self) -> Result<()> {
        let snapshot = Entry {
            prev_clock: Clock { term: 0, index: 0 },
            this_clock: Clock { term: 0, index: 1 },
            command: Command::serialize(Command::Snapshot {
                membership: HashSet::new(),
            }),
        };
        self.command_log.insert_snapshot(snapshot).await?;

        let mut membership = HashSet::new();
        membership.insert(self.driver.selfid());
        let add_server = Entry {
            prev_clock: Clock { term: 0, index: 1 },
            this_clock: Clock { term: 0, index: 2 },
            command: Command::serialize(Command::ClusterConfiguration {
                membership: membership.clone(),
            }),
        };
        self.command_log.insert_entry(add_server).await?;
        self.peers
            .set_membership(membership, 2, Ref(self.voter.clone()))
            .await?;

        // After this function is called
        // this server immediately becomes the leader by self-vote and advance commit index.
        // Consequently, when initial install_snapshot is called this server is already the leader.
        let conn = self.driver.connect(self.driver.selfid());
        conn.send_timeout_now().await?;

        Ok(())
    }

    pub(crate) async fn add_server(&self, req: request::AddServer) -> Result<()> {
        if self.peers.read_membership().is_empty() && req.server_id == self.driver.selfid() {
            // This is called the "cluster bootstrapping".
            // To add a node to a cluster we have to know some node in the cluster.
            // But what about the first node?
            self.init_cluster().await?;
        } else {
            let msg = kern_message::KernRequest::AddServer(req.server_id);
            let req = request::KernRequest {
                message: msg.serialize(),
            };
            let conn = self.driver.connect(self.driver.selfid());
            conn.process_kern_request(req).await?;
        }
        Ok(())
    }

    pub(crate) async fn remove_server(&self, req: request::RemoveServer) -> Result<()> {
        let msg = kern_message::KernRequest::RemoveServer(req.server_id);
        let req = request::KernRequest {
            message: msg.serialize(),
        };
        let conn = self.driver.connect(self.driver.selfid());
        conn.process_kern_request(req).await?;
        Ok(())
    }

    pub(crate) async fn request_cluster_info(&self) -> Result<response::ClusterInfo> {
        let ballot = self.voter.read_ballot().await?;
        let membership = self.peers.read_membership();
        let info = response::ClusterInfo {
            known_leader: ballot.voted_for,
            known_members: membership.clone(),
        };
        Ok(info)
    }
}
