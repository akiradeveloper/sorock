use super::*;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    #[error("process not found (lane_id={0})")]
    ProcessNotFound(LaneId),
    #[error("peer (node_id={0}) not found")]
    PeerNotFound(NodeId),
    #[error("log state is broken")]
    LogStateError,
    #[error("entry not found at index {0}")]
    EntryNotFound(u64),
    #[error(transparent)]
    StreamChunkError(#[from] tonic::Status),
}
