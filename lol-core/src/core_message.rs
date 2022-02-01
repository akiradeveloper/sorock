use crate::{Id, Uri};

/// Request
#[derive(serde::Serialize, serde::Deserialize, std::fmt::Debug)]
pub(crate) enum Req {
    AddServer(Id),
    RemoveServer(Id),
}
impl Req {
    pub fn serialize(x: &Self) -> Vec<u8> {
        bincode::serialize(x).unwrap()
    }
    pub fn deserialize(x: &[u8]) -> Option<Self> {
        bincode::deserialize(x).ok()
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_core_message() {
        let uri: Uri = "https://192.168.11.13:50000".parse().unwrap();
        let x = Req::AddServer(uri.into());
        let b = Req::serialize(&x);
        let y = Req::deserialize(&b).unwrap();
        assert!(std::matches!(y, Req::AddServer(_)));
    }
}
