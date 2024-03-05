use super::*;

use tokio::task::AbortHandle;

pub mod advance_commit;
pub mod advance_kern;
pub mod advance_snapshot;
pub mod advance_user;
pub mod election;
pub mod heartbeat;
pub mod log_compaction;
pub mod query_execution;
pub mod replication;
pub mod snapshot_deleter;
pub mod stepdown;

/// Wrapper around a `AbortHandle` that aborts it is dropped.
pub struct ThreadHandle(AbortHandle);
impl Drop for ThreadHandle {
    fn drop(&mut self) {
        self.0.abort();
    }
}
