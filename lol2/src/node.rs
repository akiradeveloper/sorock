use super::*;

pub struct Inner {
    selfid: NodeId,
    cache: moka::sync::Cache<NodeId, raft::RaftClient>,
    process: once_cell::sync::OnceCell<RaftProcess>,
}

#[derive(shrinkwraprs::Shrinkwrap, Clone)]
pub struct RaftNode(Arc<Inner>);
impl RaftNode {
    pub fn new(id: NodeId) -> Self {
        let builder = moka::sync::Cache::builder()
            .initial_capacity(1000)
            .time_to_live(Duration::from_secs(60));
        let inner = Inner {
            selfid: id,
            cache: builder.build(),
            process: once_cell::sync::OnceCell::new(),
        };
        Self(inner.into())
    }

    pub fn get_driver(&self, lane_id: u32) -> RaftDriver {
        RaftDriver {
            lane_id,
            self_node_id: self.selfid.clone(),
            cache: self.cache.clone(),
        }
    }

    pub fn attach_process(&self, lane_id: u32, p: RaftProcess) {
        self.process.set(p).ok();
    }

    pub(crate) fn get_process(&self, lane_id: u32) -> &RaftProcess {
        self.process.get().unwrap()
    }
}

#[derive(Clone)]
pub struct RaftDriver {
    lane_id: u32,
    self_node_id: NodeId,
    cache: moka::sync::Cache<NodeId, raft::RaftClient>,
}
impl RaftDriver {
    pub(crate) fn self_node_id(&self) -> NodeId {
        self.self_node_id.clone()
    }
    pub(crate) fn connect(&self, id: NodeId) -> requester::Connection {
        let conn = self.cache.get_with(id.clone(), || {
            let endpoint = tonic::transport::Endpoint::from(id.0);
            let chan = endpoint.connect_lazy();
            raft::RaftClient::new(chan)
        });
        requester::Connection::new(conn, self.lane_id)
    }
}
