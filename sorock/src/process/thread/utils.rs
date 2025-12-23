use super::*;

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
pub struct EventNotifier<T> {
    inner: Arc<Notify>,
    phantom: PhantomData<T>,
}

impl<T> EventNotifier<T> {
    pub fn push_event(&self, _: T) {
        self.inner.notify_waiters();
    }
}

#[derive(Clone)]
pub struct EventWaiter<T> {
    inner: Arc<Notify>,
    phantom: PhantomData<T>,
}

impl<T> EventWaiter<T> {
    /// Return if events are produced or timeout.
    pub async fn consume_events(&self, timeout: Duration) {
        tokio::time::timeout(timeout, self.inner.notified())
            .await
            .ok();
    }
}

pub fn notify<T>() -> (EventNotifier<T>, EventWaiter<T>) {
    let inner = Arc::new(Notify::new());
    (
        EventNotifier {
            inner: inner.clone(),
            phantom: PhantomData,
        },
        EventWaiter {
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
pub struct KernelQueueEvent;

#[derive(Clone)]
pub struct AppQueueEvent;

#[derive(Clone)]
pub struct AppliedEvent;
