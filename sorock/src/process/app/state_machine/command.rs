use super::*;

#[derive(serde::Serialize, serde::Deserialize)]
pub enum Command<'a> {
    /// Any append entry from a client shouldn't be processed unless consensus is reached
    /// up to the barrier entry which is queued by the current leader.
    /// This ensures the current leader has common log entries with the majority.
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

    pub fn deserialize(x: &[u8]) -> Command {
        bincode::deserialize(x).unwrap()
    }
}
