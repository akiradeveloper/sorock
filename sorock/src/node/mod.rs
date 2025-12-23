use super::*;

mod communicator;

use communicator::{Communicator, RaftConnection};
use std::collections::HashMap;

/// `RaftNode` manages multiple `RaftProcess`es on shards.
pub struct RaftNode {
    pub self_server_id: ServerAddress,
    cache: moka::sync::Cache<ServerAddress, RaftConnection>,
    process_map: parking_lot::RwLock<HashMap<ShardId, Arc<process::RaftProcess>>>,
}

impl RaftNode {
    /// Create a new Raft node with a given node ID.
    pub fn new(id: ServerAddress) -> Self {
        let builder = moka::sync::Cache::builder()
            .initial_capacity(3)
            .time_to_idle(Duration::from_secs(60));
        Self {
            self_server_id: id,
            cache: builder.build(),
            process_map: HashMap::new().into(),
        }
    }

    /// Get a `RaftIO` to give I/O capability to a Raft process on a shard.
    pub fn get_io_capability(&self, shard_id: ShardId) -> RaftIO {
        RaftIO {
            shard_id,
            self_server_id: self.self_server_id.clone(),
            connection_cache: self.cache.clone(),
        }
    }

    /// Attach a Raft process to a shard.
    pub fn attach_process(&self, shard_id: ShardId, p: process::RaftProcess) {
        self.process_map.write().insert(shard_id, Arc::new(p));
    }

    /// Detach a Raft process from a shard.
    pub fn detach_process(&self, shard_id: ShardId) {
        self.process_map.write().remove(&shard_id);
    }

    pub(super) fn get_process(&self, shard_id: ShardId) -> Option<Arc<process::RaftProcess>> {
        self.process_map.read().get(&shard_id).cloned()
    }

    pub fn list_processes(&self) -> Vec<ShardId> {
        self.process_map
            .read()
            .keys()
            .cloned()
            .collect::<Vec<ShardId>>()
    }
}

/// `RaftIO` gives I/O capability to a Raft process on a shard.
#[derive(Clone)]
pub struct RaftIO {
    pub self_server_id: ServerAddress,
    pub shard_id: ShardId,
    connection_cache: moka::sync::Cache<ServerAddress, RaftConnection>,
}

impl RaftIO {
    pub(super) fn self_server_id(&self) -> ServerAddress {
        self.self_server_id.clone()
    }

    pub(super) fn connect(&self, dest_node_id: ServerAddress) -> Communicator {
        let conn: RaftConnection = self.connection_cache.get_with(dest_node_id.clone(), || {
            RaftConnection::new(self.self_server_id.clone(), dest_node_id.clone())
        });
        Communicator::new(conn, self.shard_id)
    }
}
