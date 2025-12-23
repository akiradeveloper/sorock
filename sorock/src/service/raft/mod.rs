use super::*;

mod raft {
    tonic::include_proto!("sorock");
}
type RaftClient = raft::raft_client::RaftClient<tonic::transport::channel::Channel>;

use process::*;
use raft::raft_server::{Raft, RaftServer};
use std::pin::Pin;

pub mod client;
mod shard_table;
mod stream;

/// Create a Raft service backed by a `RaftNode`.
pub fn new(node: Arc<node::RaftNode>) -> RaftServer<impl Raft> {
    let mapping = Arc::new(parking_lot::RwLock::new(shard_table::ShardTable::new()));
    let conn_cache = moka::sync::Cache::builder()
        .initial_capacity(3)
        .time_to_idle(Duration::from_secs(60))
        .build();
    let inner = RaftService {
        node,
        mapping,
        conn_cache,
    };
    raft::raft_server::RaftServer::new(inner)
}

#[doc(hidden)]
pub struct RaftService {
    node: Arc<node::RaftNode>,
    mapping: Arc<parking_lot::RwLock<shard_table::ShardTable>>,
    conn_cache: moka::sync::Cache<ServerAddress, RaftClient>,
}

impl RaftService {
    fn connect(&self, node_id: &ServerAddress) -> client::RaftClient {
        self.conn_cache.get_with(node_id.clone(), || {
            let endpoint = tonic::transport::Endpoint::from(node_id.0.clone());
            let conn = endpoint.connect_lazy();
            RaftClient::new(conn)
        })
    }
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

        if let Some(process) = self.node.get_process(shard_id) {
            let req = request::AppWriteRequest {
                message: req.message,
                request_id: req.request_id,
            };
            let resp = process.process_app_write_request(req).await.unwrap();
            return Ok(tonic::Response::new(raft::Response { message: resp }));
        }

        // Fallback to some existing replica
        let replica0 = self.mapping.read().choose_one_replica(shard_id);
        if let Some(replica) = replica0 {
            return self.connect(&replica).write(req).await;
        }

        Err(Error::ShardUnreachable(shard_id)).unwrap()
    }

    async fn read(
        &self,
        request: tonic::Request<raft::ReadRequest>,
    ) -> std::result::Result<tonic::Response<raft::Response>, tonic::Status> {
        let req = request.into_inner();

        let shard_id = req.shard_id;

        if let Some(process) = self.node.get_process(shard_id) {
            let req = request::AppReadRequest {
                message: req.message,
            };
            let resp = process.process_app_read_request(req).await.unwrap();
            return Ok(tonic::Response::new(raft::Response { message: resp }));
        }

        // Fallback to some existing replica
        let replica0 = self.mapping.read().choose_one_replica(shard_id);
        if let Some(replica) = replica0 {
            return self.connect(&replica).read(req).await;
        }

        Err(Error::ShardUnreachable(shard_id)).unwrap()
    }

    async fn add_server(
        &self,
        request: tonic::Request<raft::AddServerRequest>,
    ) -> std::result::Result<tonic::Response<()>, tonic::Status> {
        let req = request.into_inner();
        let shard_id = req.shard_id;

        if let Some(process) = self.node.get_process(shard_id) {
            let req = request::AddServer {
                server_id: req.server_id.parse().unwrap(),
            };
            process.add_server(req).await.unwrap();
            return Ok(tonic::Response::new(()));
        }

        let replica0 = self.mapping.read().choose_one_replica(shard_id);
        if let Some(replica) = replica0 {
            return self
                .connect(&replica)
                .add_server(raft::AddServerRequest {
                    shard_id,
                    server_id: req.server_id,
                })
                .await;
        }

        Err(Error::ShardUnreachable(shard_id)).unwrap()
    }

    async fn remove_server(
        &self,
        request: tonic::Request<raft::RemoveServerRequest>,
    ) -> std::result::Result<tonic::Response<()>, tonic::Status> {
        let req = request.into_inner();
        let shard_id = req.shard_id;

        if let Some(process) = self.node.get_process(shard_id) {
            let req = request::RemoveServer {
                server_id: req.server_id.parse().unwrap(),
            };
            process.remove_server(req).await.unwrap();
            return Ok(tonic::Response::new(()));
        }

        let replica0 = self.mapping.read().choose_one_replica(shard_id);
        if let Some(replica) = replica0 {
            return self
                .connect(&replica)
                .remove_server(raft::RemoveServerRequest {
                    shard_id,
                    server_id: req.server_id,
                })
                .await;
        }

        Err(Error::ShardUnreachable(shard_id)).unwrap()
    }

    async fn get_membership(
        &self,
        req: tonic::Request<raft::Shard>,
    ) -> std::result::Result<tonic::Response<raft::Membership>, tonic::Status> {
        let req = req.into_inner();

        let shard_id = req.shard_id;

        if let Some(process) = self.node.get_process(shard_id) {
            let resp = process.get_membership().await.unwrap();
            let out = raft::Membership {
                members: resp.members.into_iter().map(|x| x.to_string()).collect(),
            };
            return Ok(tonic::Response::new(out));
        }

        // Fallback to some existing replica
        let replica0 = self.mapping.read().choose_one_replica(shard_id);
        if let Some(replica) = replica0 {
            return self
                .connect(&replica)
                .get_membership(raft::Shard { shard_id: shard_id })
                .await;
        }

        Err(Error::ShardUnreachable(shard_id)).unwrap()
    }

    async fn process_kernel_request(
        &self,
        request: tonic::Request<raft::KernelRequest>,
    ) -> std::result::Result<tonic::Response<()>, tonic::Status> {
        let req = request.into_inner();

        let shard_id = req.shard_id;

        if let Some(process) = self.node.get_process(shard_id) {
            let req = request::KernelRequest {
                message: req.message,
            };
            process.process_kernel_request(req).await.unwrap();
            return Ok(tonic::Response::new(()));
        }

        Err(Error::ProcessNotFound(shard_id)).unwrap()
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

        if let Some(process) = self.node.get_process(shard_id) {
            let resp = process.request_vote(req).await.unwrap();
            return Ok(tonic::Response::new(raft::VoteResponse {
                vote_granted: resp,
            }));
        }

        Err(Error::ProcessNotFound(shard_id)).unwrap()
    }

    async fn send_replication_stream(
        &self,
        request: tonic::Request<tonic::Streaming<raft::ReplicationStreamChunk>>,
    ) -> std::result::Result<tonic::Response<raft::ReplicationStreamResponse>, tonic::Status> {
        let st = request.into_inner();
        let (shard_id, st) = stream::into_internal_replication_stream(st).await.unwrap();

        if let Some(process) = self.node.get_process(shard_id) {
            let resp = process.send_replication_stream(st).await.unwrap();
            return Ok(tonic::Response::new(raft::ReplicationStreamResponse {
                n_inserted: resp.n_inserted,
                log_last_index: resp.log_last_index,
            }));
        }

        Err(Error::ProcessNotFound(shard_id)).unwrap()
    }

    async fn get_snapshot(
        &self,
        request: tonic::Request<raft::GetSnapshotRequest>,
    ) -> std::result::Result<tonic::Response<Self::GetSnapshotStream>, tonic::Status> {
        let req = request.into_inner();
        let shard_id = req.shard_id;

        if let Some(process) = self.node.get_process(shard_id) {
            let resp = process.get_snapshot(req.index).await.unwrap();
            let resp = stream::into_external_snapshot_stream(resp);
            return Ok(tonic::Response::new(resp));
        }

        Err(Error::ProcessNotFound(shard_id)).unwrap()
    }

    async fn send_heartbeat(
        &self,
        request: tonic::Request<raft::Heartbeat>,
    ) -> std::result::Result<tonic::Response<()>, tonic::Status> {
        let req = request.into_inner();
        let leader_id: ServerAddress = req.sender_id.parse().unwrap();

        let mut futs = vec![];
        for (shard_id, leader_state) in req.sender_commit_states {
            let req = request::Heartbeat {
                sender_term: leader_state.sender_term,
                sender_commit_index: leader_state.sender_commit_index,
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

        if let Some(process) = self.node.get_process(shard_id) {
            process.send_timeout_now().await.unwrap();
            return Ok(tonic::Response::new(()));
        }

        Err(Error::ProcessNotFound(shard_id)).unwrap()
    }

    async fn compare_term(
        &self,
        req: tonic::Request<raft::CompareTermRequest>,
    ) -> std::result::Result<tonic::Response<raft::CompareTermResponse>, tonic::Status> {
        let req = req.into_inner();
        let shard_id = req.shard_id;

        if let Some(process) = self.node.get_process(shard_id) {
            let ack = process.compare_term(req.sender_term).await.unwrap();
            return Ok(tonic::Response::new(raft::CompareTermResponse { ack }));
        }

        Err(Error::ProcessNotFound(shard_id)).unwrap()
    }

    async fn issue_read_index(
        &self,
        req: tonic::Request<raft::Shard>,
    ) -> std::result::Result<tonic::Response<raft::ReadIndex>, tonic::Status> {
        let req = req.into_inner();
        let shard_id = req.shard_id;

        if let Some(process) = self.node.get_process(shard_id) {
            let read_index = process.issue_read_index().await.unwrap();
            let out = raft::ReadIndex { read_index };
            return Ok(tonic::Response::new(out));
        }

        Err(Error::ProcessNotFound(shard_id)).unwrap()
    }

    type WatchLogMetricsStream =
        Pin<Box<dyn Stream<Item = Result<raft::LogMetrics, tonic::Status>> + Send>>;

    async fn watch_log_metrics(
        &self,
        req: tonic::Request<raft::Shard>,
    ) -> std::result::Result<tonic::Response<Self::WatchLogMetricsStream>, tonic::Status> {
        let shard_id = req.into_inner().shard_id;
        let node = self.node.clone();
        let st = async_stream::try_stream! {
            let mut intvl = tokio::time::interval(std::time::Duration::from_secs(1));
            loop {
                intvl.tick().await;

                let process = node
                    .get_process(shard_id)
                    .context(Error::ProcessNotFound(shard_id))
                    .unwrap();
                let log_state = process.get_log_state().await.unwrap();
                let metrics = raft::LogMetrics {
                    head_index: log_state.head_index,
                    snapshot_index: log_state.snapshot_index,
                    app_index: log_state.app_index,
                    commit_index: log_state.commit_index,
                    last_index: log_state.last_index,
                };
                yield metrics
            }
        };
        Ok(tonic::Response::new(Box::pin(st)))
    }

    async fn get_shard_mapping(
        &self,
        _: tonic::Request<()>,
    ) -> std::result::Result<tonic::Response<raft::ShardMapping>, tonic::Status> {
        let mut indices = vec![];
        let shards = self.node.list_processes();
        for shard in shards {
            indices.push(shard);
        }

        Ok(tonic::Response::new(raft::ShardMapping {
            server_id: self.node.self_server_id.to_string(),
            shard_indices: indices,
        }))
    }

    async fn update_shard_mapping(
        &self,
        request: tonic::Request<raft::ShardMapping>,
    ) -> std::result::Result<tonic::Response<()>, tonic::Status> {
        let req = request.into_inner();

        let node_id: ServerAddress = req.server_id.parse().unwrap();
        self.mapping
            .write()
            .update_mapping(node_id, req.shard_indices);

        Ok(tonic::Response::new(()))
    }
}
