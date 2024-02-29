use super::*;

use process::*;

mod stream;

/// Create a Raft service backed by a `RaftNode`.
pub fn new(node: RaftNode) -> raft::raft_server::RaftServer<ServiceImpl> {
    let inner = ServiceImpl { node };
    raft::raft_server::RaftServer::new(inner)
}

#[doc(hidden)]
pub struct ServiceImpl {
    node: RaftNode,
}

#[tonic::async_trait]
impl raft::raft_server::Raft for ServiceImpl {
    type GetSnapshotStream = stream::SnapshotStreamOut;

    async fn write(
        &self,
        request: tonic::Request<raft::WriteRequest>,
    ) -> std::result::Result<tonic::Response<raft::Response>, tonic::Status> {
        let req = request.into_inner();
        let lane_id = req.lane_id;
        let req = request::UserWriteRequest {
            message: req.message,
            request_id: req.request_id,
        };
        let resp = self
            .node
            .get_process(lane_id)
            .context(Error::ProcessNotFound(lane_id))
            .unwrap()
            .process_user_write_request(req)
            .await
            .unwrap();
        Ok(tonic::Response::new(raft::Response { message: resp }))
    }

    async fn read(
        &self,
        request: tonic::Request<raft::ReadRequest>,
    ) -> std::result::Result<tonic::Response<raft::Response>, tonic::Status> {
        let req = request.into_inner();
        let lane_id = req.lane_id;
        let req = request::UserReadRequest {
            message: req.message,
        };
        let resp = self
            .node
            .get_process(lane_id)
            .context(Error::ProcessNotFound(lane_id))
            .unwrap()
            .process_user_read_request(req)
            .await
            .unwrap();
        Ok(tonic::Response::new(raft::Response { message: resp }))
    }

    async fn process_kern_request(
        &self,
        request: tonic::Request<raft::KernRequest>,
    ) -> std::result::Result<tonic::Response<()>, tonic::Status> {
        let req = request.into_inner();
        let lane_id = req.lane_id;
        let req = request::KernRequest {
            message: req.message,
        };
        self.node
            .get_process(lane_id)
            .context(Error::ProcessNotFound(lane_id))
            .unwrap()
            .process_kern_request(req)
            .await
            .unwrap();
        Ok(tonic::Response::new(()))
    }

    async fn request_vote(
        &self,
        request: tonic::Request<raft::VoteRequest>,
    ) -> std::result::Result<tonic::Response<raft::VoteResponse>, tonic::Status> {
        let req = request.into_inner();
        let lane_id = req.lane_id;
        let req = request::RequestVote {
            candidate_id: req.candidate_id.parse().unwrap(),
            candidate_clock: {
                let clock = req.candidate_clock.unwrap();
                Clock {
                    term: clock.term,
                    index: clock.index,
                }
            },
            vote_term: req.vote_term,
            force_vote: req.force_vote,
            pre_vote: req.pre_vote,
        };
        let resp = self
            .node
            .get_process(lane_id)
            .context(Error::ProcessNotFound(lane_id))
            .unwrap()
            .request_vote(req)
            .await
            .unwrap();
        Ok(tonic::Response::new(raft::VoteResponse {
            vote_granted: resp,
        }))
    }

    async fn add_server(
        &self,
        request: tonic::Request<raft::AddServerRequest>,
    ) -> std::result::Result<tonic::Response<()>, tonic::Status> {
        let req = request.into_inner();
        let lane_id = req.lane_id;
        let req = request::AddServer {
            server_id: req.server_id.parse().unwrap(),
        };
        self.node
            .get_process(lane_id)
            .context(Error::ProcessNotFound(lane_id))
            .unwrap()
            .add_server(req)
            .await
            .unwrap();
        Ok(tonic::Response::new(()))
    }

    async fn remove_server(
        &self,
        request: tonic::Request<raft::RemoveServerRequest>,
    ) -> std::result::Result<tonic::Response<()>, tonic::Status> {
        let req = request.into_inner();
        let lane_id = req.lane_id;
        let req = request::RemoveServer {
            server_id: req.server_id.parse().unwrap(),
        };
        self.node
            .get_process(lane_id)
            .context(Error::ProcessNotFound(lane_id))
            .unwrap()
            .remove_server(req)
            .await
            .unwrap();
        Ok(tonic::Response::new(()))
    }

    async fn send_replication_stream(
        &self,
        request: tonic::Request<tonic::Streaming<raft::ReplicationStreamChunk>>,
    ) -> std::result::Result<tonic::Response<raft::ReplicationStreamResponse>, tonic::Status> {
        let st = request.into_inner();
        let (lane_id, st) = stream::into_internal_log_stream(st).await;
        let resp = self
            .node
            .get_process(lane_id)
            .context(Error::ProcessNotFound(lane_id))
            .unwrap()
            .send_log_stream(st)
            .await
            .unwrap();
        Ok(tonic::Response::new(raft::ReplicationStreamResponse {
            n_inserted: resp.n_inserted,
            log_last_index: resp.log_last_index,
        }))
    }

    async fn get_snapshot(
        &self,
        request: tonic::Request<raft::GetSnapshotRequest>,
    ) -> std::result::Result<tonic::Response<Self::GetSnapshotStream>, tonic::Status> {
        let req = request.into_inner();
        let lane_id = req.lane_id;
        let resp = self
            .node
            .get_process(lane_id)
            .context(Error::ProcessNotFound(lane_id))
            .unwrap()
            .get_snapshot(req.index)
            .await
            .unwrap();
        let resp = stream::into_external_snapshot_stream(resp);
        Ok(tonic::Response::new(resp))
    }

    async fn send_heartbeat(
        &self,
        request: tonic::Request<raft::Heartbeat>,
    ) -> std::result::Result<tonic::Response<()>, tonic::Status> {
        let req = request.into_inner();
        let lane_id = req.lane_id;
        let req = request::Heartbeat {
            leader_id: req.leader_id.parse().unwrap(),
            leader_term: req.leader_term,
            leader_commit_index: req.leader_commit_index,
        };
        self.node
            .get_process(lane_id)
            .context(Error::ProcessNotFound(lane_id))
            .unwrap()
            .send_heartbeat(req)
            .await
            .unwrap();
        Ok(tonic::Response::new(()))
    }

    async fn timeout_now(
        &self,
        req: tonic::Request<raft::TimeoutNowRequest>,
    ) -> std::result::Result<tonic::Response<()>, tonic::Status> {
        let req = req.into_inner();
        let lane_id = req.lane_id;
        self.node
            .get_process(lane_id)
            .context(Error::ProcessNotFound(lane_id))
            .unwrap()
            .send_timeout_now()
            .await
            .unwrap();
        Ok(tonic::Response::new(()))
    }
}
