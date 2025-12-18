use super::*;

pub struct Query {
    pub message: Bytes,
    pub app_completion: completion::ApplicationCompletion,
}

#[derive(Clone)]
pub struct PendingQueue {
    inner: Arc<spin::Mutex<Vec<Query>>>,
}
impl PendingQueue {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(spin::Mutex::new(vec![])),
        }
    }

    pub fn queue(&self, q: Query) -> Result<()> {
        self.inner.lock().push(q);
        Ok(())
    }

    pub fn drain(&self) -> Vec<Query> {
        let mut inner = self.inner.lock();
        let out = inner.drain(..).collect();
        out
    }

    pub fn requeue(&self, qs: Vec<Query>) {
        let mut inner = self.inner.lock();
        for q in qs {
            inner.push(q);
        }
    }
}

struct WaitQueue<T> {
    inner: BTreeMap<LogIndex, Vec<T>>,
}
impl<T> WaitQueue<T> {
    fn new() -> Self {
        Self {
            inner: BTreeMap::new(),
        }
    }

    fn push(&mut self, index: LogIndex, q: T) {
        self.inner.entry(index).or_default().push(q);
    }

    fn pop(&mut self, upto: LogIndex) -> Vec<(LogIndex, T)> {
        let mut out = vec![];

        let mut del_keys = vec![];
        for (&i, _) in self.inner.range(..=upto) {
            del_keys.push(i);
        }

        for i in del_keys {
            let qs = self.inner.remove(&i).unwrap();
            for q in qs {
                out.push((i, q));
            }
        }

        out
    }
}

#[derive(Clone)]
pub struct ReadyQueue {
    app: Read<App>,
    wait_queue: Arc<spin::Mutex<WaitQueue<Query>>>,
}
impl ReadyQueue {
    pub fn new(app: Read<App>) -> Self {
        Self {
            app,
            wait_queue: Arc::new(spin::Mutex::new(WaitQueue::new())),
        }
    }

    pub fn register(&self, read_index: LogIndex, qs: Vec<Query>) {
        let mut wait_queue = self.wait_queue.lock();
        for q in qs {
            wait_queue.push(read_index, q);
        }
    }

    /// Process the waiting queries up to `readable_index`.
    pub async fn process(&self, readable_index: LogIndex) -> usize {
        let qs = self.wait_queue.lock().pop(readable_index);

        let mut futs = vec![];
        for (_, q) in qs {
            let app = self.app.clone();
            let fut = async move {
                // The `completion` of the failed queries are dropped
                // which just results in failing on the client side.
                if let Ok(resp) = app.process_read(&q.message).await {
                    q.app_completion.complete_with(resp).ok();
                }
            };
            futs.push(fut);
        }
        let n = futs.len();
        futures::future::join_all(futs).await;

        n
    }
}
