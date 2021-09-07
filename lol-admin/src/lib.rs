#[derive(serde::Serialize)]
pub struct ClusterInfo {
    pub leader_id: Option<String>,
    pub membership: Vec<String>,
}

#[derive(serde::Serialize)]
pub struct Config {
    pub compaction_delay_sec: u64,
    pub compaction_interval_sec: u64,
}
