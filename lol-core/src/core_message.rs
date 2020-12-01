use crate::Id;

/// Request
#[derive(serde::Serialize, serde::Deserialize, std::fmt::Debug)]
pub enum Req {
    AddServer(Id),
    RemoveServer(Id),
    ClusterInfo,
    LogInfo,
    HealthCheck,
}
/// Reply
#[derive(serde::Serialize, serde::Deserialize, std::fmt::Debug)]
pub enum Rep {
    ClusterInfo {
        leader_id: Option<Id>,
        membership: Vec<Id>,
    },
    LogInfo {
        snapshot_index: u64,
        last_applied: u64,
        commit_index: u64,
        last_log_index: u64,
    },
    HealthCheck {
        ok: bool,
    },
    // TODO: Tune
}
impl Req {
    pub fn serialize(x: &Self) -> Vec<u8> {
        rmp_serde::to_vec(x).unwrap()
    }
    pub fn deserialize(x: &[u8]) -> Option<Self> {
        rmp_serde::from_slice(x).ok()
    }
}
impl Rep {
    pub fn serialize(x: &Self) -> Vec<u8> {
        rmp_serde::to_vec(x).unwrap()
    }
    pub fn deserialize(x: &[u8]) -> Option<Self> {
        rmp_serde::from_slice(x).ok()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_core_message() {
        let x = Req::AddServer("192.168.11.13".to_owned());
        let b = Req::serialize(&x);
        let y = Req::deserialize(&b).unwrap();
        assert!(std::matches!(y, Req::AddServer(_)));
    }
}
