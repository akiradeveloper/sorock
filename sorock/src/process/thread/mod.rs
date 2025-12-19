use super::*;

pub mod advance_application;
pub mod advance_commit;
pub mod advance_kernel;
pub mod advance_snapshot;
pub mod delete_old_entries;
pub mod delete_old_snapshots;
pub mod election;
pub mod query_execution;
pub mod query_queue_coordinator;
pub mod stepdown;

use tokio::task::AbortHandle;

/// Wrapper around a `AbortHandle` that aborts it is dropped.
pub struct ThreadHandle(pub AbortHandle);
impl Drop for ThreadHandle {
    fn drop(&mut self) {
        self.0.abort();
    }
}

use std::marker::PhantomData;
use std::sync::Arc;
use tokio::sync::Notify;

#[derive(Clone)]
pub struct EventProducer<T> {
    inner: Arc<Notify>,
    phantom: PhantomData<T>,
}
impl<T> EventProducer<T> {
    pub fn push_event(&self, _: T) {
        self.inner.notify_one();
    }
}

#[derive(Clone)]
pub struct EventConsumer<T> {
    inner: Arc<Notify>,
    phantom: PhantomData<T>,
}
impl<T> EventConsumer<T> {
    /// Return if events are produced or timeout.
    pub async fn consume_events(&self, timeout: Duration) {
        tokio::time::timeout(timeout, self.inner.notified())
            .await
            .ok();
    }
}

pub fn notify<T>() -> (EventProducer<T>, EventConsumer<T>) {
    let inner = Arc::new(Notify::new());
    (
        EventProducer {
            inner: inner.clone(),
            phantom: PhantomData,
        },
        EventConsumer {
            inner,
            phantom: PhantomData,
        },
    )
}

#[derive(Clone)]
pub struct QueueEvent;

#[derive(Clone)]
pub struct ReplicationEvent;

#[derive(Clone)]
pub struct CommitEvent;

#[derive(Clone)]
pub struct KernEvent;

#[derive(Clone)]
pub struct ApplicationEvent;
