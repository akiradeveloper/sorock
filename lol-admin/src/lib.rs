#[derive(serde::Serialize, serde::Deserialize, std::fmt::Debug, Clone, Eq)]
pub struct ClusterInfo {
    pub leader_id: Option<String>,
    pub membership: Vec<String>,
}
impl PartialEq for ClusterInfo {
    fn eq(&self, other: &Self) -> bool {
        self.leader_id == other.leader_id && self.membership == other.membership
    }
}