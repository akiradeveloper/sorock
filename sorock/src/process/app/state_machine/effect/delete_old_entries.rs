use super::*;

/// Delete log entries in `[, snapshot_index)`.
pub struct Effect {
    pub state_machine: StateMachine,
}
impl Effect {
    pub async fn exec(self) -> Result<()> {
        let cur_snapshot_index = self.state_machine.snapshot_pointer.load(Ordering::SeqCst);
        self.state_machine
            .storage
            .delete_entries_before(cur_snapshot_index)
            .await?;
        Ok(())
    }
}
