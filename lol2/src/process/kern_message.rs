use super::*;

#[derive(serde::Serialize, serde::Deserialize, std::fmt::Debug)]
pub enum KernRequest {
    AddServer(NodeId),
    RemoveServer(NodeId),
    Noop,
}

impl KernRequest {
    pub fn serialize(self) -> Bytes {
        bincode::serialize(&self).unwrap().into()
    }

    pub fn deserialize(x: &[u8]) -> Option<Self> {
        bincode::deserialize(x).ok()
    }
}
