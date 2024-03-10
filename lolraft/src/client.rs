use super::*;

/// The client for the Raft service.
pub type RaftClient = raft::raft_client::RaftClient<tonic::transport::channel::Channel>;

pub use raft::{
    AddServerRequest, ReadRequest, RemoveServerRequest, Response, TimeoutNow, WriteRequest,
};
