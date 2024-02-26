use super::*;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    #[error("peer (id={0}) not found")]
    PeerNotFound(NodeId),
    #[error("log state is broken")]
    LogStateError,
    #[error("entry not found at index {0}")]
    EntryNotFound(u64),
}
