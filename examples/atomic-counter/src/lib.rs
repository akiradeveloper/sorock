#[derive(serde::Serialize, serde::Deserialize)]
pub enum Req {
    IncAndGet,
    Get,
}

#[derive(serde::Serialize, serde::Deserialize)]
pub enum Rep {
    IncAndGet(u64),
    Get(u64),
}
impl Req {
    pub fn serialize(msg: &Req) -> Vec<u8> {
        bincode::serialize(msg).unwrap()
    }
    pub fn deserialize(b: &[u8]) -> Option<Self> {
        bincode::deserialize(b).ok()
    }
}
impl Rep {
    pub fn serialize(msg: &Rep) -> Vec<u8> {
        bincode::serialize(msg).unwrap()
    }
    pub fn deserialize(b: &[u8]) -> Option<Self> {
        bincode::deserialize(b).ok()
    }
}
