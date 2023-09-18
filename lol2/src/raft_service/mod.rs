use super::*;

use process::*;

mod stream;

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

    async fn process(
        &self,
        request: tonic::Request<raft::Request>,
    ) -> std::result::Result<tonic::Response<raft::Response>, tonic::Status> {
        let req = request.into_inner();
        let req = request::UserRequest {
            message: req.message,
            mutation: req.mutation,
        };
        let resp = self
            .node
            .get_process()
            .process_user_request(req)
            .await
            .unwrap();
        Ok(tonic::Response::new(raft::Response { message: resp }))
    }

    async fn process_kern_request(
        &self,
        request: tonic::Request<raft::KernRequest>,
    ) -> std::result::Result<tonic::Response<()>, tonic::Status> {
        let req = request.into_inner();
        let req = request::KernRequest {
            message: req.message,
        };
        self.node
            .get_process()
            .process_kern_request(req)
            .await
            .unwrap();
        Ok(tonic::Response::new(()))
    }

    async fn noop(
        &self,
        _: tonic::Request<()>,
    ) -> std::result::Result<tonic::Response<()>, tonic::Status> {
        self.node.get_process().noop().await.unwrap();
        Ok(tonic::Response::new(()))
    }

    async fn get_cluster_info(
        &self,
        _: tonic::Request<()>,
    ) -> std::result::Result<tonic::Response<raft::ClusterInfo>, tonic::Status> {
        let resp = self
            .node
            .get_process()
            .request_cluster_info()
            .await
            .unwrap();
        Ok(tonic::Response::new(raft::ClusterInfo {
            known_leader_id: resp.known_leader.map(|id| id.to_string()),
            known_members: resp
                .known_members
                .into_iter()
                .map(|id| id.to_string())
                .collect(),
        }))
    }

    async fn request_vote(
        &self,
        request: tonic::Request<raft::VoteRequest>,
    ) -> std::result::Result<tonic::Response<raft::VoteResponse>, tonic::Status> {
        let req = request.into_inner();
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
        let resp = self.node.get_process().request_vote(req).await.unwrap();
        Ok(tonic::Response::new(raft::VoteResponse {
            vote_granted: resp,
        }))
    }

    async fn add_server(
        &self,
        request: tonic::Request<raft::AddServerRequest>,
    ) -> std::result::Result<tonic::Response<()>, tonic::Status> {
        let req = request.into_inner();
        let req = request::AddServer {
            server_id: req.server_id.parse().unwrap(),
        };
        self.node.get_process().add_server(req).await.unwrap();
        Ok(tonic::Response::new(()))
    }

    async fn remove_server(
        &self,
        request: tonic::Request<raft::RemoveServerRequest>,
    ) -> std::result::Result<tonic::Response<()>, tonic::Status> {
        let req = request.into_inner();
        let req = request::RemoveServer {
            server_id: req.server_id.parse().unwrap(),
        };
        self.node.get_process().remove_server(req).await.unwrap();
        Ok(tonic::Response::new(()))
    }

    async fn send_log_stream(
        &self,
        request: tonic::Request<tonic::Streaming<raft::LogStreamChunk>>,
    ) -> std::result::Result<tonic::Response<raft::SendLogStreamResponse>, tonic::Status> {
        let st = request.into_inner();
        let st = stream::into_internal_log_stream(st).await;
        let resp = self.node.get_process().send_log_stream(st).await.unwrap();
        Ok(tonic::Response::new(raft::SendLogStreamResponse {
            success: resp.success,
            log_last_index: resp.log_last_index,
        }))
    }

    async fn get_snapshot(
        &self,
        request: tonic::Request<raft::GetSnapshotRequest>,
    ) -> std::result::Result<tonic::Response<Self::GetSnapshotStream>, tonic::Status> {
        let req = request.into_inner();
        let resp = self
            .node
            .get_process()
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
        let req = request::Heartbeat {
            leader_id: req.leader_id.parse().unwrap(),
            leader_term: req.leader_term,
            leader_commit_index: req.leader_commit_index,
        };
        self.node.get_process().send_heartbeat(req).await.unwrap();
        Ok(tonic::Response::new(()))
    }

    async fn timeout_now(
        &self,
        _: tonic::Request<()>,
    ) -> std::result::Result<tonic::Response<()>, tonic::Status> {
        self.node.get_process().send_timeout_now().await.unwrap();
        Ok(tonic::Response::new(()))
    }
}
