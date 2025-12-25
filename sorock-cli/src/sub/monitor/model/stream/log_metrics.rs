use super::*;

pub struct CopyLogMetrics {
    pub url: Uri,
}
impl CopyLogMetrics {
    pub async fn copy(
        &mut self,
        st: impl Stream<Item = sorock::LogMetrics>,
        data: Arc<RwLock<Nodes>>,
    ) {
        let mut st = Box::pin(st);
        while let Some(metric) = st.next().await {
            if let Some(state) = data.write().nodes.get_mut(&self.url) {
                let new_state = LogState {
                    snapshot_index: metric.snapshot_index,
                    // These indices can be behind snapshot_index for a short time.
                    // (e.g. after snapshot entry is inserted but before applied)
                    app_index: u64::max(metric.app_index, metric.snapshot_index),
                    commit_index: u64::max(metric.commit_index, metric.snapshot_index),
                    last_index: metric.last_index,
                };
                state.log_state = new_state;
            }
        }
    }
}
