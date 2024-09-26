use super::*;

use communicator::{Communicator, RaftConnection};
use std::collections::HashMap;

pub struct Inner {
    self_node_id: NodeId,
    cache: moka::sync::Cache<NodeId, RaftConnection>,
    process: spin::RwLock<HashMap<ShardId, Arc<RaftProcess>>>,
}

/// `RaftNode` contains a set of `RaftProcess`es.
#[derive(shrinkwraprs::Shrinkwrap, Clone)]
pub struct RaftNode(Arc<Inner>);
impl RaftNode {
    /// Create a new Raft node with a given node ID.
    pub fn new(id: NodeId) -> Self {
        let builder = moka::sync::Cache::builder()
            .initial_capacity(3)
            .time_to_idle(Duration::from_secs(60));
        let inner = Inner {
            self_node_id: id,
            cache: builder.build(),
            process: HashMap::new().into(),
        };
        Self(inner.into())
    }

    /// Get a Raft driver to drive a Raft process on a shard.
    pub fn get_driver(&self, shard_id: ShardId) -> RaftDriver {
        RaftDriver {
            shard_id,
            self_node_id: self.self_node_id.clone(),
            connection_cache: self.cache.clone(),
        }
    }

    /// Attach a Raft process to a shard.
    pub fn attach_process(&self, shard_id: ShardId, p: RaftProcess) {
        self.process.write().insert(shard_id, Arc::new(p));
    }

    /// Detach a Raft process from a shard.
    pub fn detach_process(&self, shard_id: ShardId) {
        self.process.write().remove(&shard_id);
    }

    pub(crate) fn get_process(&self, shard_id: ShardId) -> Option<Arc<RaftProcess>> {
        self.process.read().get(&shard_id).cloned()
    }
}

/// `RaftDriver` is a context to drive a `RaftProcess`.
#[derive(Clone)]
pub struct RaftDriver {
    shard_id: ShardId,
    self_node_id: NodeId,
    connection_cache: moka::sync::Cache<NodeId, RaftConnection>,
}
impl RaftDriver {
    pub(crate) fn self_node_id(&self) -> NodeId {
        self.self_node_id.clone()
    }

    pub(crate) fn connect(&self, dest_node_id: NodeId) -> Communicator {
        let conn: RaftConnection = self.connection_cache.get_with(dest_node_id.clone(), || {
            RaftConnection::new(self.self_node_id.clone(), dest_node_id.clone())
        });
        Communicator::new(conn, self.shard_id)
    }
}
