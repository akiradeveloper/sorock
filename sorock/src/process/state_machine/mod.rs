use super::*;

pub mod command_exec;
pub mod command_log;
pub mod completion;
pub mod query_queue;

#[derive(Deref, Clone)]
pub struct App(Arc<dyn RaftApp>);
impl App {
    pub fn new(x: impl RaftApp) -> Self {
        Self(Arc::new(x))
    }

    pub async fn fetch_snapshot(
        &self,
        snapshot_index: LogIndex,
        owner: ServerAddress,
        io: RaftIO,
    ) -> Result<()> {
        if owner == io.self_server_id() {
            return Ok(());
        }
        if snapshot_index == 1 {
            return Ok(());
        }
        let conn = io.connect(owner);
        let st = conn.get_snapshot(snapshot_index).await?;
        self.save_snapshot(st, snapshot_index).await?;
        Ok(())
    }

    pub async fn apply_snapshot(&self, snapshot_index: LogIndex) -> Result<()> {
        // The initial snapshot is implicit.
        // The state machine should be initialized accordingly.
        if snapshot_index == 1 {
            return Ok(());
        }
        info!("install snapshot@{snapshot_index}");
        self.install_snapshot(snapshot_index).await?;
        Ok(())
    }
}
