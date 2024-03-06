use super::*;

use std::collections::BTreeMap;

pub struct Query {
    pub message: Bytes,
    pub user_completion: completion::UserCompletion,
}

pub struct Inner {
    app: Ref<App>,
    q: spin::Mutex<Impl>,
}

#[derive(shrinkwraprs::Shrinkwrap, Clone)]
pub struct QueryQueue(Arc<Inner>);
impl QueryQueue {
    pub fn new(app: Ref<App>) -> Self {
        let inner = Inner {
            app,
            q: Impl::new().into(),
        };
        Self(inner.into())
    }
}

impl QueryQueue {
    /// Register a query to be executed when the read index reaches `read_index`.
    /// `read_index` is the index of the commit pointer of when the query is submitted.
    pub fn register(&self, read_index: Index, query: Query) {
        let mut q = self.q.lock();
        q.register(read_index, query);
    }

    /// Execute awaiting queries in `[, index]` in parallel.
    pub fn execute(&self, index: Index) -> bool {
        let mut q = self.q.lock();
        q.execute(index, &self.app)
    }
}

pub struct Impl {
    reserved: BTreeMap<Index, Vec<Query>>,
}

impl Impl {
    fn new() -> Self {
        Self {
            reserved: BTreeMap::new(),
        }
    }

    fn register(&mut self, read_index: Index, query: Query) {
        self.reserved
            .entry(read_index)
            .or_insert(vec![])
            .push(query);
    }

    fn execute(&mut self, index: Index, app: &App) -> bool {
        let futs = {
            let mut out = vec![];
            let ls: Vec<Index> = self.reserved.range(..=index).map(|(k, _)| *k).collect();
            for idx in ls {
                if let Some(queries) = self.reserved.remove(&idx) {
                    for query in queries {
                        out.push((query, app.clone()));
                    }
                }
            }
            out
        };

        if futs.is_empty() {
            return false;
        }

        let futs = futs.into_iter().map(
            |(
                Query {
                    message,
                    user_completion,
                },
                app,
            )| async move {
                // The `completion` of the failed queries are dropped
                // which just results in failing on the client side.
                if let Ok(resp) = app.process_read(&message).await {
                    user_completion.complete_with(resp).ok();
                }
            },
        );

        for fut in futs {
            tokio::spawn(fut);
        }

        true
    }
}
