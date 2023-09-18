use super::*;

#[derive(serde::Serialize, serde::Deserialize)]
pub enum Command<'a> {
    Noop,
    Snapshot {
        membership: HashSet<NodeId>,
    },
    ClusterConfiguration {
        membership: HashSet<NodeId>,
    },
    Req {
        #[serde(with = "serde_bytes")]
        message: &'a [u8],
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
