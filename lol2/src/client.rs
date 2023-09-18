use super::*;

pub type RaftClient = raft::raft_client::RaftClient<tonic::transport::channel::Channel>;
pub use raft::{AddServerRequest, ClusterInfo, RemoveServerRequest, Request, Response};
