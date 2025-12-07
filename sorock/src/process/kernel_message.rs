use super::*;

#[derive(serde::Serialize, serde::Deserialize, std::fmt::Debug)]
pub enum KernelMessage {
    AddServer(NodeAddress),
    RemoveServer(NodeAddress),
}
impl KernelMessage {
    pub fn serialize(self) -> Bytes {
        bincode::serialize(&self).unwrap().into()
    }

    pub fn deserialize(x: &[u8]) -> Option<Self> {
        bincode::deserialize(x).ok()
    }
}
