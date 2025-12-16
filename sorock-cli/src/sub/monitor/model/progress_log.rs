use super::*;

pub struct ProgressLog {
    log: BTreeMap<Instant, u64>,
}
impl ProgressLog {
    pub fn new() -> Self {
        Self {
            log: BTreeMap::new(),
        }
    }
    pub fn get_range(&self, start: Instant, end: Instant) -> BTreeMap<Instant, u64> {
        self.log.range(start..=end).map(|(&k, &v)| (k, v)).collect()
    }
    pub fn test() -> Self {
        let mut log = BTreeMap::new();
        let now = Instant::now();
        for i in 0..100000 {
            log.insert(now + Duration::from_secs(i), i * i);
        }
        Self { log }
    }
}

pub fn copy(nodes: Arc<RwLock<Nodes>>, progress_log: Arc<RwLock<ProgressLog>>) {
    let nodes = nodes.read();
    let mut progress_log = progress_log.write();
    let max_value = nodes
        .nodes
        .values()
        .map(|node_state| node_state.log_state.commit_index)
        .max()
        .unwrap_or(0);
    progress_log.log.insert(Instant::now(), max_value);
}
