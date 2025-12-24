use super::*;

#[derive(Serialize, Deserialize, std::fmt::Debug)]
pub enum KernelMessage {
    AddServer(ServerAddress, bool),
    RemoveServer(ServerAddress),
}

impl KernelMessage {
    pub fn serialize(self) -> Bytes {
        bincode::serialize(&self).unwrap().into()
    }

    pub fn deserialize(x: &[u8]) -> Option<Self> {
        bincode::deserialize(x).ok()
    }
}
