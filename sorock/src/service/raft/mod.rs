use super::*;

mod raft {
    tonic::include_proto!("sorock");
}
type RaftClient = raft::raft_client::RaftClient<tonic::transport::channel::Channel>;

use process::*;
use raft::raft_server::{Raft, RaftServer};
use std::pin::Pin;
use tonic::Status;

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

fn into_status(e: anyhow::Error) -> Status {
    if let Some(e) = e.downcast_ref::<Error>() {
        return match e {
            Error::BadReplicationStream | Error::BadKernelMessage | Error::BadSnapshotChunk(_) => {
                Status::invalid_argument(e.to_string())
            }
            Error::SnapshotNotFound(_) | Error::EntryNotFound(_) | Error::ProcessNotFound(_) => {
                Status::not_found(e.to_string())
            }
            Error::LeaderUnknown | Error::ShardUnreachable(_) => Status::unavailable(e.to_string()),
            Error::BadLogState => Status::internal(e.to_string()),
        };
    }

    Status::internal(e.to_string())
}

fn parse_server_address(input: &str, field: &str) -> std::result::Result<ServerAddress, Status> {
    input
        .parse()
        .map_err(|e| Status::invalid_argument(format!("invalid {field}: {e}")))
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
            let resp = process
                .process_app_write_request(req)
                .await
                .map_err(into_status)?;
            return Ok(tonic::Response::new(raft::Response { message: resp }));
        }

        // Fallback to some existing replica
        let replica0 = self.mapping.read().choose_one_replica(shard_id);
        if let Some(replica) = replica0 {
            return self.connect(&replica).write(req).await;
        }

        Err(Status::unavailable(
            Error::ShardUnreachable(shard_id).to_string(),
        ))
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
            let resp = process
                .process_app_read_request(req)
                .await
                .map_err(into_status)?;
            return Ok(tonic::Response::new(raft::Response { message: resp }));
        }

        // Fallback to some existing replica
        let replica0 = self.mapping.read().choose_one_replica(shard_id);
        if let Some(replica) = replica0 {
            return self.connect(&replica).read(req).await;
        }

        Err(Status::unavailable(
            Error::ShardUnreachable(shard_id).to_string(),
        ))
    }

    async fn add_server(
        &self,
        request: tonic::Request<raft::AddServerRequest>,
    ) -> std::result::Result<tonic::Response<()>, tonic::Status> {
        let req = request.into_inner();
        let shard_id = req.shard_id;

        if let Some(process) = self.node.get_process(shard_id) {
            let req = request::AddServer {
                server_id: parse_server_address(&req.server_id, "server_id")?,
                as_voter: req.as_voter,
            };
            process.add_server(req).await.map_err(into_status)?;
            return Ok(tonic::Response::new(()));
        }

        let replica0 = self.mapping.read().choose_one_replica(shard_id);
        if let Some(replica) = replica0 {
            return self
                .connect(&replica)
                .add_server(raft::AddServerRequest {
                    shard_id,
                    server_id: req.server_id,
                    as_voter: req.as_voter,
                })
                .await;
        }

        Err(Status::unavailable(
            Error::ShardUnreachable(shard_id).to_string(),
        ))
    }

    async fn remove_server(
        &self,
        request: tonic::Request<raft::RemoveServerRequest>,
    ) -> std::result::Result<tonic::Response<()>, tonic::Status> {
        let req = request.into_inner();
        let shard_id = req.shard_id;

        if let Some(process) = self.node.get_process(shard_id) {
            let req = request::RemoveServer {
                server_id: parse_server_address(&req.server_id, "server_id")?,
            };
            process.remove_server(req).await.map_err(into_status)?;
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

        Err(Status::unavailable(
            Error::ShardUnreachable(shard_id).to_string(),
        ))
    }

    async fn get_membership(
        &self,
        req: tonic::Request<raft::Shard>,
    ) -> std::result::Result<tonic::Response<raft::Membership>, tonic::Status> {
        let req = req.into_inner();

        let shard_id = req.shard_id;

        if let Some(process) = self.node.get_process(shard_id) {
            let resp = process.get_membership().await.map_err(into_status)?;
            let out = raft::Membership {
                members: resp
                    .members
                    .into_iter()
                    .map(|(k, v)| (k.to_string(), v))
                    .collect(),
                leader_id: resp.leader_id.to_string(),
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

        Err(Status::unavailable(
            Error::ShardUnreachable(shard_id).to_string(),
        ))
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
            process
                .process_kernel_request(req)
                .await
                .map_err(into_status)?;
            return Ok(tonic::Response::new(()));
        }

        Err(Status::not_found(
            Error::ProcessNotFound(shard_id).to_string(),
        ))
    }

    async fn request_vote(
        &self,
        request: tonic::Request<raft::VoteRequest>,
    ) -> std::result::Result<tonic::Response<raft::VoteResponse>, tonic::Status> {
        let req = request.into_inner();
        let shard_id = req.shard_id;
        let req = request::RequestVote {
            candidate_id: parse_server_address(&req.candidate_id, "candidate_id")?,
            candidate_clock: {
                let clock = req
                    .candidate_clock
                    .ok_or_else(|| Status::invalid_argument("missing candidate_clock"))?;
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
            let resp = process.request_vote(req).await.map_err(into_status)?;
            return Ok(tonic::Response::new(raft::VoteResponse {
                vote_granted: resp,
            }));
        }

        Err(Status::not_found(
            Error::ProcessNotFound(shard_id).to_string(),
        ))
    }

    async fn send_replication_stream(
        &self,
        request: tonic::Request<tonic::Streaming<raft::ReplicationStreamChunk>>,
    ) -> std::result::Result<tonic::Response<raft::ReplicationStreamResponse>, tonic::Status> {
        let st = request.into_inner();
        let (shard_id, st) = stream::into_internal_replication_stream(st)
            .await
            .map_err(into_status)?;

        if let Some(process) = self.node.get_process(shard_id) {
            let resp = process
                .send_replication_stream(st)
                .await
                .map_err(into_status)?;
            return Ok(tonic::Response::new(raft::ReplicationStreamResponse {
                n_inserted: resp.n_inserted,
                log_last_index: resp.log_last_index,
            }));
        }

        Err(Status::not_found(
            Error::ProcessNotFound(shard_id).to_string(),
        ))
    }

    async fn get_snapshot(
        &self,
        request: tonic::Request<raft::GetSnapshotRequest>,
    ) -> std::result::Result<tonic::Response<Self::GetSnapshotStream>, tonic::Status> {
        let req = request.into_inner();
        let shard_id = req.shard_id;

        if let Some(process) = self.node.get_process(shard_id) {
            let resp = process.get_snapshot(req.index).await.map_err(into_status)?;
            let resp = stream::into_external_snapshot_stream(resp);
            return Ok(tonic::Response::new(resp));
        }

        Err(Status::not_found(
            Error::ProcessNotFound(shard_id).to_string(),
        ))
    }

    async fn send_heartbeat(
        &self,
        request: tonic::Request<raft::Heartbeat>,
    ) -> std::result::Result<tonic::Response<()>, tonic::Status> {
        let req = request.into_inner();
        let leader_id = parse_server_address(&req.sender_id, "sender_id")?;

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
        futures::future::try_join_all(futs)
            .await
            .map_err(into_status)?;
        Ok(tonic::Response::new(()))
    }

    async fn send_timeout_now(
        &self,
        req: tonic::Request<raft::TimeoutNow>,
    ) -> std::result::Result<tonic::Response<()>, tonic::Status> {
        let req = req.into_inner();
        let shard_id = req.shard_id;

        if let Some(process) = self.node.get_process(shard_id) {
            process.send_timeout_now().await.map_err(into_status)?;
            return Ok(tonic::Response::new(()));
        }

        Err(Status::not_found(
            Error::ProcessNotFound(shard_id).to_string(),
        ))
    }

    async fn compare_term(
        &self,
        req: tonic::Request<raft::CompareTermRequest>,
    ) -> std::result::Result<tonic::Response<raft::CompareTermResponse>, tonic::Status> {
        let req = req.into_inner();
        let shard_id = req.shard_id;

        if let Some(process) = self.node.get_process(shard_id) {
            let ack = process
                .compare_term(req.sender_term)
                .await
                .map_err(into_status)?;
            return Ok(tonic::Response::new(raft::CompareTermResponse { ack }));
        }

        Err(Status::not_found(
            Error::ProcessNotFound(shard_id).to_string(),
        ))
    }

    async fn issue_read_index(
        &self,
        req: tonic::Request<raft::Shard>,
    ) -> std::result::Result<tonic::Response<raft::ReadIndex>, tonic::Status> {
        let req = req.into_inner();
        let shard_id = req.shard_id;

        if let Some(process) = self.node.get_process(shard_id) {
            let read_index = process.issue_read_index().await.map_err(into_status)?;
            let out = raft::ReadIndex { read_index };
            return Ok(tonic::Response::new(out));
        }

        Err(Status::not_found(
            Error::ProcessNotFound(shard_id).to_string(),
        ))
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
                    .map_err(into_status)?;
                let log_state = process.get_log_state().await.map_err(into_status)?;
                let metrics = raft::LogMetrics {
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

        let node_id = parse_server_address(&req.server_id, "server_id")?;
        self.mapping
            .write()
            .update_mapping(node_id, req.shard_indices);

        Ok(tonic::Response::new(()))
    }
}
