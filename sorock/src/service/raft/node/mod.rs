use super::*;

mod communicator;

use communicator::{Communicator, RaftConnection};
use std::collections::HashMap;

pub struct Inner {
    self_node_id: NodeAddress,
    cache: moka::sync::Cache<NodeAddress, RaftConnection>,
    process_map: spin::RwLock<HashMap<ShardIndex, Arc<RaftProcess>>>,
}

/// `RaftNode` contains a set of `RaftProcess`es.
#[derive(Deref, Clone)]
pub struct RaftNode(Arc<Inner>);
impl RaftNode {
    /// Create a new Raft node with a given node ID.
    pub fn new(id: NodeAddress) -> Self {
        let builder = moka::sync::Cache::builder()
            .initial_capacity(3)
            .time_to_idle(Duration::from_secs(60));
        let inner = Inner {
            self_node_id: id,
            cache: builder.build(),
            process_map: HashMap::new().into(),
        };
        Self(inner.into())
    }

    /// Get a Raft driver to drive a Raft process on a shard.
    pub fn get_handle(&self, shard_index: ShardIndex) -> RaftHandle {
        RaftHandle {
            shard_index,
            self_node_id: self.self_node_id.clone(),
            connection_cache: self.cache.clone(),
        }
    }

    /// Attach a Raft process to a shard.
    pub fn attach_process(&self, shard_index: ShardIndex, p: RaftProcess) {
        self.process_map.write().insert(shard_index, Arc::new(p));
    }

    /// Detach a Raft process from a shard.
    pub fn detach_process(&self, shard_index: ShardIndex) {
        self.process_map.write().remove(&shard_index);
    }

    pub(crate) fn get_process(&self, shard_index: ShardIndex) -> Option<Arc<RaftProcess>> {
        self.process_map.read().get(&shard_index).cloned()
    }
}

/// `RaftDriver` is a context to drive a `RaftProcess`.
#[derive(Clone)]
pub struct RaftHandle {
    shard_index: ShardIndex,
    self_node_id: NodeAddress,
    connection_cache: moka::sync::Cache<NodeAddress, RaftConnection>,
}
impl RaftHandle {
    pub(crate) fn self_node_id(&self) -> NodeAddress {
        self.self_node_id.clone()
    }

    pub(crate) fn connect(&self, dest_node_id: NodeAddress) -> Communicator {
        let conn: RaftConnection = self.connection_cache.get_with(dest_node_id.clone(), || {
            RaftConnection::new(self.self_node_id.clone(), dest_node_id.clone())
        });
        Communicator::new(conn, self.shard_index)
    }
}
