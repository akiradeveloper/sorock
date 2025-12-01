use super::*;

mod raft {
    tonic::include_proto!("sorock");
    pub type RaftClient = raft_client::RaftClient<tonic::transport::channel::Channel>;
}

use process::*;
use raft::raft_server::{Raft, RaftServer};

pub mod client;
pub(crate) mod communicator;
mod stream;

/// Create a Raft service backed by a `RaftNode`.
pub fn new(node: RaftNode) -> RaftServer<impl Raft> {
    let inner = RaftService { node };
    raft::raft_server::RaftServer::new(inner)
}

#[doc(hidden)]
pub struct RaftService {
    node: RaftNode,
}

#[tonic::async_trait]
impl raft::raft_server::Raft for RaftService {
    type GetSnapshotStream = stream::SnapshotStreamOut;

    async fn write(
        &self,
        request: tonic::Request<raft::WriteRequest>,
    ) -> std::result::Result<tonic::Response<raft::Response>, tonic::Status> {
        let req = request.into_inner();
        let shard_id = req.shard_id;
        let req = request::UserWriteRequest {
            message: req.message,
            request_id: req.request_id,
        };
        let resp = self
            .node
            .get_process(shard_id)
            .context(Error::ProcessNotFound(shard_id))
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
        let shard_id = req.shard_id;
        let req = request::UserReadRequest {
            message: req.message,
            read_locally: req.read_locally,
        };
        let resp = self
            .node
            .get_process(shard_id)
            .context(Error::ProcessNotFound(shard_id))
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
        let shard_id = req.shard_id;
        let req = request::KernRequest {
            message: req.message,
        };
        self.node
            .get_process(shard_id)
            .context(Error::ProcessNotFound(shard_id))
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
        let shard_id = req.shard_id;
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
            .get_process(shard_id)
            .context(Error::ProcessNotFound(shard_id))
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
        let shard_id = req.shard_id;
        let req = request::AddServer {
            server_id: req.server_id.parse().unwrap(),
        };
        self.node
            .get_process(shard_id)
            .context(Error::ProcessNotFound(shard_id))
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
        let shard_id = req.shard_id;
        let req = request::RemoveServer {
            server_id: req.server_id.parse().unwrap(),
        };
        self.node
            .get_process(shard_id)
            .context(Error::ProcessNotFound(shard_id))
            .unwrap()
            .remove_server(req)
            .await
            .unwrap();
        Ok(tonic::Response::new(()))
    }

    async fn get_membership(
        &self,
        req: tonic::Request<raft::Shard>,
    ) -> std::result::Result<tonic::Response<raft::Membership>, tonic::Status> {
        let shard_id = req.into_inner().id;
        let process = self
            .node
            .get_process(shard_id)
            .context(Error::ProcessNotFound(shard_id))
            .unwrap();
        let members = process.get_membership().await.unwrap().members;
        let out = raft::Membership {
            members: members.into_iter().map(|x| x.to_string()).collect(),
        };
        Ok(tonic::Response::new(out))
    }

    async fn send_replication_stream(
        &self,
        request: tonic::Request<tonic::Streaming<raft::ReplicationStreamChunk>>,
    ) -> std::result::Result<tonic::Response<raft::ReplicationStreamResponse>, tonic::Status> {
        let st = request.into_inner();
        let (shard_id, st) = stream::into_internal_replication_stream(st).await.unwrap();
        let resp = self
            .node
            .get_process(shard_id)
            .context(Error::ProcessNotFound(shard_id))
            .unwrap()
            .send_replication_stream(st)
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
        let shard_id = req.shard_id;
        let resp = self
            .node
            .get_process(shard_id)
            .context(Error::ProcessNotFound(shard_id))
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
        let leader_id: NodeId = req.leader_id.parse().unwrap();

        let mut futs = vec![];
        for (shard_id, leader_state) in req.leader_commit_states {
            let req = request::Heartbeat {
                leader_term: leader_state.leader_term,
                leader_commit_index: leader_state.leader_commit_index,
            };
            if let Some(process) = self.node.get_process(shard_id) {
                let leader_id = leader_id.clone();
                futs.push(async move { process.receive_heartbeat(leader_id, req).await });
            }
        }
        futures::future::try_join_all(futs).await.unwrap();
        Ok(tonic::Response::new(()))
    }

    async fn send_timeout_now(
        &self,
        req: tonic::Request<raft::TimeoutNow>,
    ) -> std::result::Result<tonic::Response<()>, tonic::Status> {
        let req = req.into_inner();
        let shard_id = req.shard_id;
        self.node
            .get_process(shard_id)
            .context(Error::ProcessNotFound(shard_id))
            .unwrap()
            .send_timeout_now()
            .await
            .unwrap();
        Ok(tonic::Response::new(()))
    }
}
