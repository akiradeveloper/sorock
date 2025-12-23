use super::*;

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

pub struct QueryExec {
    app: App,
    wait_queue: WaitQueue<Query>,
}
impl QueryExec {
    pub fn new(app: App) -> Self {
        Self {
            app,
            wait_queue: WaitQueue::new(),
        }
    }

    pub fn register(&mut self, read_index: LogIndex, qs: Vec<Query>) {
        for q in qs {
            self.wait_queue.push(read_index, q);
        }
    }

    /// Process the waiting queries up to `readable_index`.
    pub async fn process(&mut self, readable_index: LogIndex) -> usize {
        let qs = self.wait_queue.pop(readable_index);

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
