use crate::{ack, Index, RaftApp, RaftCore};
use bytes::Bytes;
use std::collections::BTreeMap;
use std::sync::Arc;

pub struct Query {
    pub core: bool,
    pub message: Bytes,
    pub ack: ack::Ack,
}
pub struct QueryQueue {
    reserved: BTreeMap<Index, Vec<Query>>,
}
impl QueryQueue {
    pub fn new() -> Self {
        Self {
            reserved: BTreeMap::new(),
        }
    }
    pub fn register(&mut self, idx: Index, query: Query) {
        self.reserved.entry(idx).or_insert(vec![]).push(query);
    }
    pub async fn execute(&mut self, idx: Index, raft_core: Arc<RaftCore>) -> bool {
        let mut futs = vec![];
        let ls: Vec<Index> = self.reserved.range(..=idx).map(|(k, _)| *k).collect();
        for idx in ls {
            if let Some(queries) = self.reserved.remove(&idx) {
                for query in queries {
                    futs.push((query, raft_core.clone()));
                }
            }
        }
        if futs.is_empty() {
            return false;
        }
        let futs = futs
            .into_iter()
            .map(|(Query { core, message, ack }, raft_core)| async move {
                assert_eq!(core, false);
                let res = raft_core.app.read_message(&message).await;
                if let ack::Ack::OnApply(tx) = ack {
                    if let Ok(msg) = res {
                        let _ = tx.send(ack::ApplyOk(msg));
                    }
                } else {
                    unreachable!()
                }
            });
        for fut in futs {
            tokio::spawn(fut);
        }

        true
    }
}
