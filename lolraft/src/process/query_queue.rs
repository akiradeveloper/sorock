use super::*;

struct Queue<T> {
    inner: BTreeMap<Index, Vec<T>>,
}
impl<T> Queue<T> {
    fn new() -> Self {
        Self {
            inner: BTreeMap::new(),
        }
    }

    fn push(&mut self, index: Index, q: T) {
        self.inner.entry(index).or_default().push(q);
    }

    fn pop(&mut self, upto: Index) -> Vec<(Index, T)> {
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
    pub user_completion: completion::UserCompletion,
}

#[derive(Clone)]
pub struct Producer {
    inner: Arc<spin::Mutex<Queue<Query>>>,
}
impl Producer {
    /// Register a query for execution when the readable index reaches `read_index`.
    /// `read_index` should be the index of the commit pointer at the time of query.
    pub fn register(&self, read_index: Index, q: Query) -> Result<()> {
        self.inner.lock().push(read_index, q);
        Ok(())
    }
}

#[derive(Clone)]
pub struct Processor {
    app: Ref<App>,
    inner: Arc<spin::Mutex<Queue<Query>>>,
}
impl Processor {
    /// Process the waiting queries up to `readable_index`.
    pub fn process(&self, readable_index: Index) -> usize {
        let qs = self.inner.lock().pop(readable_index);

        let mut n = 0;
        for (_, q) in qs {
            n += 1;
            let app = self.app.clone();
            let fut = async move {
                // The `completion` of the failed queries are dropped
                // which just results in failing on the client side.
                if let Ok(resp) = app.process_read(&q.message).await {
                    q.user_completion.complete_with(resp).ok();
                }
            };
            tokio::spawn(fut);
        }

        n
    }
}

pub fn new(app: Ref<App>) -> (Producer, Processor) {
    let q = Arc::new(spin::Mutex::new(Queue::new()));
    let processor = Processor {
        inner: q.clone(),
        app,
    };
    let producer = Producer { inner: q.clone() };
    (producer, processor)
}
