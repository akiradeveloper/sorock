use super::*;

#[derive(Deref, DerefMut)]
pub struct App {
    #[deref]
    #[deref_mut]
    app: Box<dyn RaftApp>,

    io: RaftIO,
}

impl App {
    pub fn new(app: impl RaftApp, io: RaftIO) -> Self {
        Self {
            app: Box::new(app),
            io,
        }
    }

    pub async fn fetch_snapshot(
        &mut self,
        snapshot_index: LogIndex,
        owner: ServerAddress,
    ) -> Result<()> {
        if owner == self.io.self_server_id() {
            return Ok(());
        }

        if snapshot_index == 1 {
            return Ok(());
        }

        let conn = self.io.connect(owner);
        let st = conn.get_snapshot(snapshot_index).await?;
        self.app.save_snapshot(st, snapshot_index).await?;

        Ok(())
    }

    pub async fn apply_snapshot(&mut self, snapshot_index: LogIndex) -> Result<()> {
        // The initial snapshot is implicit.
        // The state machine should be initialized accordingly.
        if snapshot_index == 1 {
            return Ok(());
        }

        info!("install snapshot@{snapshot_index}");
        self.app.install_snapshot(snapshot_index).await?;

        Ok(())
    }
}
