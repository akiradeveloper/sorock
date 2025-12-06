use super::*;

#[derive(serde::Serialize, serde::Deserialize, std::fmt::Debug)]
pub enum KernelRequest {
    AddServer(NodeId),
    RemoveServer(NodeId),
}
impl KernelRequest {
    pub fn serialize(self) -> Bytes {
        bincode::serialize(&self).unwrap().into()
    }

    pub fn deserialize(x: &[u8]) -> Option<Self> {
        bincode::deserialize(x).ok()
    }
}
