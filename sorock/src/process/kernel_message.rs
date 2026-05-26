use super::*;

#[derive(Serialize, Deserialize, std::fmt::Debug)]
pub enum KernelMessage {
    AddServer(ServerAddress, bool),
    RemoveServer(ServerAddress),
}

impl KernelMessage {
    pub fn serialize(self) -> Bytes {
        postcard::to_stdvec(&self).unwrap().into()
    }

    pub fn deserialize(x: &[u8]) -> Option<Self> {
        postcard::from_bytes(x).ok()
    }
}
