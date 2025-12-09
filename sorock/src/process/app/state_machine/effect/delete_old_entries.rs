use super::*;

/// Delete log entries in `[, snapshot_index)`.
pub struct Effect {
    pub state_mechine: StateMachine,
}
impl Effect {
    pub async fn exec(self) -> Result<()> {
        let cur_snapshot_index = self.state_mechine.snapshot_pointer.load(Ordering::SeqCst);
        self.state_mechine
            .storage
            .delete_entries_before(cur_snapshot_index)
            .await?;
        Ok(())
    }
}
