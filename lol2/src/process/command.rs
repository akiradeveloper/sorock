use super::*;

#[derive(serde::Serialize, serde::Deserialize)]
pub enum Command<'a> {
    Barrier(Term),
    ClusterConfiguration {
        membership: HashSet<NodeId>,
    },
    Snapshot {
        membership: HashSet<NodeId>,
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
