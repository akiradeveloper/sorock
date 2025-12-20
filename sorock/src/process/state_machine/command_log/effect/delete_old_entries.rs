use super::*;

/// Delete log entries in `[, snapshot_index)`.
pub struct Effect<'a> {
    pub command_log: &'a CommandLog,
}
impl Effect<'_> {
    pub async fn exec(self) -> Result<()> {
        let cur_snapshot_index = self.command_log.snapshot_pointer;
        self.command_log
            .storage
            .delete_entries_before(cur_snapshot_index)
            .await?;
        Ok(())
    }
}
