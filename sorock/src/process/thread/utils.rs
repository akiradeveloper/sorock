use super::*;

use std::marker::PhantomData;
use tokio::sync::watch;
use tokio::task::AbortHandle;

/// Wrapper around a `AbortHandle` that aborts it is dropped.
pub struct ThreadHandle(pub AbortHandle);

impl Drop for ThreadHandle {
    fn drop(&mut self) {
        self.0.abort();
    }
}

#[derive(Clone)]
pub struct EventNotifier<T> {
    tx: watch::Sender<u64>,
    phantom: PhantomData<T>,
}

impl<T> EventNotifier<T> {
    pub fn push_event(&self, _: T) {
        let cur = *self.tx.borrow();
        let _ = self.tx.send(cur.wrapping_add(1));
    }
}

#[derive(Clone)]
pub struct EventWaiter<T> {
    rx: watch::Receiver<u64>,
    phantom: PhantomData<T>,
}

impl<T> EventWaiter<T> {
    /// Return if events are produced or timeout.
    pub async fn consume_events(&mut self, timeout: Duration) {
        let _ = tokio::time::timeout(timeout, self.rx.changed()).await;
    }
}

pub fn notify<T>() -> (EventNotifier<T>, EventWaiter<T>) {
    let (tx, rx) = watch::channel(0u64);
    (
        EventNotifier {
            tx,
            phantom: PhantomData,
        },
        EventWaiter {
            rx,
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
