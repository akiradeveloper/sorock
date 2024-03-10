use super::*;

use std::collections::HashMap;

pub struct Inner {
    self_node_id: NodeId,
    cache: moka::sync::Cache<NodeId, raft::RaftClient>,
    process: spin::RwLock<HashMap<LaneId, Arc<RaftProcess>>>,
}

/// `RaftNode` contains a set of `RaftProcess`es.
#[derive(shrinkwraprs::Shrinkwrap, Clone)]
pub struct RaftNode(Arc<Inner>);
impl RaftNode {
    /// Create a new Raft node with a given node ID.
    pub fn new(id: NodeId) -> Self {
        let builder = moka::sync::Cache::builder()
            .initial_capacity(1000)
            .time_to_live(Duration::from_secs(60));
        let inner = Inner {
            self_node_id: id,
            cache: builder.build(),
            process: HashMap::new().into(),
        };
        Self(inner.into())
    }

    /// Get a Raft driver to drive a Raft process on a lane.
    pub fn get_driver(&self, lane_id: LaneId) -> RaftDriver {
        RaftDriver {
            lane_id,
            self_node_id: self.self_node_id.clone(),
            cache: self.cache.clone(),
        }
    }

    /// Attach a Raft process to a lane.
    pub fn attach_process(&self, lane_id: LaneId, p: RaftProcess) {
        self.process.write().insert(lane_id, Arc::new(p));
    }

    /// Detach a Raft process from a lane.
    pub fn detach_process(&self, lane_id: LaneId) {
        self.process.write().remove(&lane_id);
    }

    pub(crate) fn get_process(&self, lane_id: LaneId) -> Option<Arc<RaftProcess>> {
        self.process.read().get(&lane_id).cloned()
    }
}

/// `RaftDriver` is a context to drive a `RaftProcess`.
#[derive(Clone)]
pub struct RaftDriver {
    lane_id: LaneId,
    self_node_id: NodeId,
    cache: moka::sync::Cache<NodeId, raft::RaftClient>,
}
impl RaftDriver {
    pub(crate) fn self_node_id(&self) -> NodeId {
        self.self_node_id.clone()
    }

    pub(crate) fn connect(&self, id: NodeId) -> communicator::Communicator {
        let conn = self.cache.get_with(id.clone(), || {
            let endpoint = tonic::transport::Endpoint::from(id.0);
            let chan = endpoint.connect_lazy();
            raft::RaftClient::new(chan)
        });
        communicator::Communicator::new(conn, self.lane_id)
    }
}
