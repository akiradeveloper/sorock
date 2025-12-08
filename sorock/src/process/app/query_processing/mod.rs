use super::*;

struct Queue<T> {
    inner: BTreeMap<LogIndex, Vec<T>>,
}
impl<T> Queue<T> {
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

pub struct Query {
    pub message: Bytes,
    pub app_completion: completion::ApplicationCompletion,
}

#[derive(Clone)]
pub struct QueryQueue {
    inner: Arc<spin::Mutex<Queue<Query>>>,
}
impl QueryQueue {
    /// Register a query for execution when the readable index reaches `read_index`.
    /// `read_index` should be the index of the commit pointer at the time of query.
    pub fn register(&self, read_index: LogIndex, q: Query) -> Result<()> {
        self.inner.lock().push(read_index, q);
        Ok(())
    }
}

#[derive(Clone)]
pub struct QueryProcessor {
    app: Read<App>,
    inner: Arc<spin::Mutex<Queue<Query>>>,
}
impl QueryProcessor {
    /// Process the waiting queries up to `readable_index`.
    pub async fn process(&self, readable_index: LogIndex) -> usize {
        let qs = self.inner.lock().pop(readable_index);

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

pub fn new(app: Read<App>) -> (QueryQueue, QueryProcessor) {
    let q = Arc::new(spin::Mutex::new(Queue::new()));
    let processor = QueryProcessor {
        inner: q.clone(),
        app,
    };
    let producer = QueryQueue { inner: q.clone() };
    (producer, processor)
}
