use super::*;

#[derive(Serialize, Deserialize)]
pub enum Command<'a> {
    /// Any append entry from a client shouldn't be processed unless consensus is
    /// reached up to the barrier entry which is queued by the current leader.
    /// This ensures committed entries will not be reverted as described in Figure 3.7
    /// by allowing only who owns the barrier entry can be elected as a new leader.
    Barrier(Term),
    ClusterConfiguration {
        membership: HashSet<NodeAddress>,
    },
    Snapshot {
        membership: HashSet<NodeAddress>,
    },
    ExecuteRequest {
        #[serde(with = "serde_bytes")]
        message: &'a [u8],
        request_id: String,
    },
    CompleteRequest {
        request_id: String,
    },
}

impl<'a> Command<'a> {
    pub fn serialize(self) -> Bytes {
        bincode::serialize(&self).unwrap().into()
    }

    pub fn deserialize(x: &[u8]) -> Command<'_> {
        bincode::deserialize(x).unwrap()
    }
}
