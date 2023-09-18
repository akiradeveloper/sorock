use super::*;

#[derive(shrinkwraprs::Shrinkwrap, Clone)]
pub struct App(Arc<dyn RaftApp>);
impl App {
    pub fn new(x: impl RaftApp) -> Self {
        Self(Arc::new(x))
    }

    pub async fn fetch_snapshot(
        &self,
        index: Index,
        owner: NodeId,
        driver: RaftDriver,
    ) -> Result<()> {
        if owner == driver.selfid() {
            return Ok(());
        }
        if index < 2 {
            return Ok(());
        }
        let conn = driver.connect(owner);
        let st = conn.get_snapshot(index).await?;
        self.save_snapshot(st, index).await?;
        Ok(())
    }
}
