use super::*;

pub struct CopyLogMetrics {
    pub url: Uri,
}
impl CopyLogMetrics {
    pub async fn copy(&mut self, st: impl Stream<Item = proto::LogMetrics>, data: Arc<RwLock<Nodes>>) -> Result<()> {
        let mut st = Box::pin(st);
        while let Some(metric) = st.next().await {
            if let Some(state) = data.write().nodes.get_mut(&self.url) {
                let new_state = LogState {
                    head_index: metric.head_index,
                    snapshot_index: metric.snap_index,
                    app_index: metric.app_index,
                    commit_index: metric.commit_index,
                    last_index: metric.last_index,
                };
                state.log_state = new_state;
            }
        }
        Ok(())
    }
}
