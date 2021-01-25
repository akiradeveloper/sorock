use bytes::Bytes;
use std::collections::BTreeMap;

#[derive(serde::Serialize, serde::Deserialize, std::fmt::Debug)]
pub struct Get(pub Option<String>);
#[derive(serde::Serialize, serde::Deserialize, std::fmt::Debug)]
pub struct List(pub Vec<(String, String)>);

#[derive(serde::Serialize, serde::Deserialize, std::fmt::Debug)]
pub enum Req {
    Set { key: String, value: String },
    SetBytes { key: String, value: Bytes },
    Get { key: String },
    List,
}
#[derive(serde::Serialize, serde::Deserialize, std::fmt::Debug)]
pub enum Rep {
    Set {},
    Get { found: bool, value: String },
    List { values: Vec<(String, String)> },
}
#[derive(serde::Serialize, serde::Deserialize, std::fmt::Debug)]
pub struct Snapshot {
    pub h: BTreeMap<String, Bytes>,
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
impl Snapshot {
    pub fn serialize(msg: &Snapshot) -> Vec<u8> {
        bincode::serialize(msg).unwrap()
    }
    pub fn deserialize(b: &[u8]) -> Option<Snapshot> {
        bincode::deserialize(b).ok()
    }
}
