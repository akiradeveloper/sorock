use super::*;

pub struct Query {
    pub message: Bytes,
    pub user_completion: completion::UserCompletion,
}

#[derive(Clone)]
pub struct Producer {
    inner: flume::Sender<(Index, Query)>,
}
impl Producer {
    /// Register a query to be executed when the read index reaches `read_index`.
    /// `read_index` is the index of the commit pointer of when the query is submitted.
    pub fn register(&self, read_index: Index, q: Query) -> Result<()> {
        self.inner.send((read_index, q))?;
        Ok(())
    }
}

#[derive(Clone)]
pub struct Processor {
    app: Ref<App>,
    inner: flume::Receiver<(Index, Query)>,
}
impl Processor {
    /// Register a query to be executed when the read index reaches `read_index`.
    /// `read_index` is the index of the commit pointer of when the query is submitted.
    pub fn process(&self, index: Index) -> usize {
        let qs = self
            .inner
            .try_iter()
            .take_while(|(read_index, _)| *read_index <= index);

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
    let (tx, rx) = flume::unbounded();
    let processor = Processor { inner: rx, app };
    let producer = Producer { inner: tx };
    (producer, processor)
}
