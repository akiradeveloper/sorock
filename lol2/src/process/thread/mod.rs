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

/// wrapper of a `AbortHandle` that aborts it is dropped.
pub struct ThreadHandle(AbortHandle);
impl Drop for ThreadHandle {
    fn drop(&mut self) {
        self.0.abort();
    }
}

/// Wrap a future which `may` panic.
/// Some part of the code in lol may panic due to
/// `intentionally` accessing to non-existing log entries.
pub async fn defensive_panic_guard<T>(future: T) -> Result<T::Output>
where
    T: Future + Send + 'static,
    T::Output: Send + 'static,
{
    let r = tokio::spawn(future).await;
    match r {
        Ok(x) => Ok(x),
        Err(e) => {
            warn!("thread panicked: {:?}", e);
            Err(e.into())
        }
    }
}
