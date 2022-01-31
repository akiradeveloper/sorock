#[derive(serde::Serialize)]
pub struct ClusterInfo {
    pub leader_id: Option<String>,
    pub membership: Vec<String>,
}

#[derive(serde::Serialize)]
pub struct Config {
    pub compaction_interval_sec: u64,
}

#[derive(serde::Serialize)]
pub struct Status {
    pub snapshot_index: u64,
    pub last_applied: u64,
    pub commit_index: u64,
    pub last_log_index: u64,
}
