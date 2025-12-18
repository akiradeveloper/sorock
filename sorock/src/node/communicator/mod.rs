use super::*;

mod heartbeat_multiplex;
mod stream;

use heartbeat_multiplex::*;
use process::*;
use std::sync::Arc;
use tokio::task::AbortHandle;

mod raft {
    tonic::include_proto!("sorock");
    pub type RaftClient = raft_client::RaftClient<tonic::transport::channel::Channel>;
}

pub struct HandleDrop(AbortHandle);
impl Drop for HandleDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
}

#[derive(Clone)]
pub struct RaftConnection {
    client: raft::RaftClient,
    heartbeat_buffer: Arc<HeartbeatBuffer>,
    _abort_hdl: Arc<HandleDrop>,
}
impl RaftConnection {
    pub fn new(self_node_id: NodeAddress, dest_node_id: NodeAddress) -> Self {
        let client = {
            let endpoint = tonic::transport::Endpoint::from(dest_node_id.0.clone())
                // (http2) Send ping to keep connection (default: disabled)
                .http2_keep_alive_interval(Duration::from_secs(1))
                // (http2) Send ping even if there is no active streams (default: disabled)
                .keep_alive_while_idle(true);

            let chan = endpoint.connect_lazy();
            raft::RaftClient::new(chan)
        };

        let heartbeat_buffer = Arc::new(HeartbeatBuffer::new());

        let fut = heartbeat_multiplex::run(heartbeat_buffer.clone(), client.clone(), self_node_id);
        let abort_hdl = tokio::spawn(fut).abort_handle();

        Self {
            client,
            heartbeat_buffer,
            _abort_hdl: Arc::new(HandleDrop(abort_hdl)),
        }
    }
}

pub struct Communicator {
    conn: RaftConnection,
    shard_index: ShardIndex,
}
impl Communicator {
    pub fn new(conn: RaftConnection, shard_index: ShardIndex) -> Self {
        Self { conn, shard_index }
    }
}

impl Communicator {
    pub async fn get_snapshot(&self, index: LogIndex) -> Result<SnapshotStream> {
        let req = raft::GetSnapshotRequest {
            shard_index: self.shard_index,
            index,
        };
        let st = self
            .conn
            .client
            .clone()
            .get_snapshot(req)
            .await?
            .into_inner();
        let st = Box::pin(stream::into_internal_snapshot_stream(st));
        Ok(st)
    }

    pub fn queue_heartbeat(&self, req: request::Heartbeat) {
        self.conn.heartbeat_buffer.push(self.shard_index, req);
    }

    pub async fn process_application_write_request(
        &self,
        req: request::ApplicationWriteRequest,
    ) -> Result<Bytes> {
        let req = raft::WriteRequest {
            shard_index: self.shard_index,
            message: req.message,
            request_id: req.request_id,
        };
        let resp = self.conn.client.clone().write(req).await?.into_inner();
        Ok(resp.message)
    }

    pub async fn process_application_read_request(
        &self,
        req: request::ApplicationReadRequest,
    ) -> Result<Bytes> {
        let req = raft::ReadRequest {
            shard_index: self.shard_index,
            message: req.message,
        };
        let resp = self.conn.client.clone().read(req).await?.into_inner();
        Ok(resp.message)
    }

    pub async fn process_kernel_request(&self, req: request::KernelRequest) -> Result<()> {
        let req = raft::KernelRequest {
            shard_index: self.shard_index,
            message: req.message,
        };
        self.conn.client.clone().process_kernel_request(req).await?;
        Ok(())
    }

    pub async fn send_timeout_now(&self) -> Result<()> {
        let req = raft::TimeoutNow {
            shard_index: self.shard_index,
        };
        self.conn.client.clone().send_timeout_now(req).await?;
        Ok(())
    }

    pub async fn send_replication_stream(
        &self,
        st: request::ReplicationStream,
    ) -> Result<response::ReplicationStream> {
        let st = stream::into_external_replication_stream(self.shard_index, st);
        let resp = self
            .conn
            .client
            .clone()
            .send_replication_stream(st)
            .await?
            .into_inner();
        Ok(response::ReplicationStream {
            n_inserted: resp.n_inserted,
            log_last_index: resp.log_last_index,
        })
    }

    pub async fn request_vote(&self, req: request::RequestVote) -> Result<bool> {
        let req = raft::VoteRequest {
            shard_index: self.shard_index,
            candidate_id: req.candidate_id.to_string(),
            candidate_clock: {
                let e = req.candidate_clock;
                Some(raft::Clock {
                    term: e.term,
                    index: e.index,
                })
            },
            vote_term: req.vote_term,
            force_vote: req.force_vote,
            pre_vote: req.pre_vote,
        };
        let resp = self
            .conn
            .client
            .clone()
            .request_vote(req)
            .await?
            .into_inner();
        Ok(resp.vote_granted)
    }

    pub async fn compare_term(&self, term: Term) -> Result<bool> {
        let req = raft::CompareTermRequest {
            shard_index: self.shard_index,
            sender_term: term,
        };
        let resp = self
            .conn
            .client
            .clone()
            .compare_term(req)
            .await?
            .into_inner();
        Ok(resp.ack)
    }

    pub async fn issue_read_index(&self) -> Result<Option<LogIndex>> {
        let req = raft::Shard {
            id: self.shard_index,
        };
        let resp = self
            .conn
            .client
            .clone()
            .issue_read_index(req)
            .await?
            .into_inner();

        Ok(resp.read_index)
    }

    pub async fn get_membership(&self) -> Result<HashSet<NodeAddress>> {
        let req = raft::Shard {
            id: self.shard_index,
        };
        let resp = self
            .conn
            .client
            .clone()
            .get_membership(req)
            .await?
            .into_inner();

        let mut out = HashSet::new();
        for m in resp.members {
            out.insert(m.parse().unwrap());
        }

        Ok(out)
    }
}
