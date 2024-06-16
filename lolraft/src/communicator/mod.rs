use super::*;

mod heartbeat_multiplex;
mod stream;

use heartbeat_multiplex::*;
use process::*;
use spin::Mutex;
use std::sync::Arc;
use tokio::task::AbortHandle;

pub struct HandleDrop(AbortHandle);
impl Drop for HandleDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
}

#[derive(Clone)]
pub struct RaftConnection {
    client: raft::RaftClient,
    heartbeat_buffer: Arc<Mutex<HeartbeatBuffer>>,
    _abort_hdl: Arc<HandleDrop>,
}
impl RaftConnection {
    pub fn new(self_node_id: NodeId, dest_node_id: NodeId) -> Self {
        let client = {
            let endpoint = tonic::transport::Endpoint::from(dest_node_id.0.clone())
                .buffer_size(1 << 16)
                // (http2) Send ping to keep connection (default: disabled)
                .http2_keep_alive_interval(Duration::from_secs(1))
                // (http2) Send ping even if there is no active streams (default: disabled)
                .keep_alive_while_idle(true);

            let chan = endpoint.connect_lazy();
            raft::RaftClient::new(chan)
        };

        let heartbeat_buffer = Arc::new(Mutex::new(HeartbeatBuffer::new()));

        let fut = heartbeat_multiplex::run(heartbeat_buffer.clone(), client.clone(), self_node_id);
        let fut = tokio::task::unconstrained(fut);
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
    lane_id: LaneId,
}
impl Communicator {
    pub fn new(conn: RaftConnection, lane_id: LaneId) -> Self {
        Self { conn, lane_id }
    }
}

impl Communicator {
    pub async fn get_snapshot(&self, index: Index) -> Result<SnapshotStream> {
        let req = raft::GetSnapshotRequest {
            lane_id: self.lane_id,
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
        self.conn.heartbeat_buffer.lock().push(self.lane_id, req);
    }

    pub async fn process_user_write_request(
        &self,
        req: request::UserWriteRequest,
    ) -> Result<Bytes> {
        let req = raft::WriteRequest {
            lane_id: self.lane_id,
            message: req.message,
            request_id: req.request_id,
        };
        let resp = self.conn.client.clone().write(req).await?.into_inner();
        Ok(resp.message)
    }

    pub async fn process_user_read_request(&self, req: request::UserReadRequest) -> Result<Bytes> {
        let req = raft::ReadRequest {
            lane_id: self.lane_id,
            message: req.message,
        };
        let resp = self.conn.client.clone().read(req).await?.into_inner();
        Ok(resp.message)
    }

    pub async fn process_kern_request(&self, req: request::KernRequest) -> Result<()> {
        let req = raft::KernRequest {
            lane_id: self.lane_id,
            message: req.message,
        };
        self.conn.client.clone().process_kern_request(req).await?;
        Ok(())
    }

    pub async fn send_timeout_now(&self) -> Result<()> {
        let req = raft::TimeoutNow {
            lane_id: self.lane_id,
        };
        self.conn.client.clone().send_timeout_now(req).await?;
        Ok(())
    }

    pub async fn send_replication_stream(
        &self,
        st: request::ReplicationStream,
    ) -> Result<response::ReplicationStream> {
        let st = stream::into_external_replication_stream(self.lane_id, st);
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
            lane_id: self.lane_id,
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
}
