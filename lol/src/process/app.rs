use super::*;

#[derive(shrinkwraprs::Shrinkwrap, Clone)]
pub struct App(Arc<dyn RaftApp>);
impl App {
    pub fn new(x: impl RaftApp) -> Self {
        Self(Arc::new(x))
    }

    pub async fn fetch_snapshot(
        &self,
        snapshot_index: Index,
        owner: NodeId,
        driver: RaftDriver,
    ) -> Result<()> {
        if owner == driver.self_node_id() {
            return Ok(());
        }
        // Every node should have a snapshot at index 1
        // because it is the initial state of the application.
        if snapshot_index == 1 {
            return Ok(());
        }
        let conn = driver.connect(owner);
        let st = conn.get_snapshot(snapshot_index).await?;
        self.save_snapshot(st, snapshot_index).await?;
        Ok(())
    }

    pub async fn apply_snapshot(&self, snapshot_index: Index) -> Result<()> {
        if snapshot_index == 1 {
            return Ok(());
        }
        self.install_snapshot(snapshot_index).await?;
        Ok(())
    }
}
