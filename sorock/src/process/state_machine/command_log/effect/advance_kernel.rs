use super::*;

pub struct Effect<'a> {
    pub command_log: &'a mut CommandLog,
    pub ctrl: ControlActor,
}

impl Effect<'_> {
    /// Advance kernel process once.
    pub async fn exec(self) -> Result<()> {
        let cur_kern_index = self.command_log.kernel_pointer;

        // We need to try-lock here to avoid ABBA deadlocks.
        // If a process try to start election and try to advance kernel pointer simultaneously, the situation may occur.
        //
        // try-promote    : LOCK(Control)  LOCK(CommandLog)
        // advance-kernel :       LOCK(CommandLog)   LOCK(Control)
        ensure!(cur_kern_index < self.ctrl.try_read()?.commit_pointer);

        let process_index = cur_kern_index + 1;
        let e = self.command_log.get_entry(process_index).await?;
        let command = Command::deserialize(&e.command);

        let do_process = match command {
            Command::Barrier { .. } => true,
            Command::ClusterConfiguration { .. } => true,
            _ => false,
        };

        if do_process {
            debug!("process kern@{process_index}");
            match command {
                Command::Barrier(term) => {
                    self.ctrl.try_write()?.commit_safe_term(term);
                }
                Command::ClusterConfiguration { .. } => {}
                _ => {}
            }
            if let Some(kern_completion) =
                self.command_log.kernel_completions.remove(&process_index)
            {
                kern_completion.complete();
            }
        }

        self.command_log.kernel_pointer = process_index;

        Ok(())
    }
}
