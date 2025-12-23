use super::*;

pub struct Effect<'a> {
    pub command_log: &'a mut CommandLog,
    pub ctrl_actor: Actor<Control>,
}

impl Effect<'_> {
    /// Advance kernel process once.
    pub async fn exec(self) -> Result<Option<command_exec::KernelCommand>> {
        let cur_kern_index = self.command_log.kernel_pointer;

        // We need to try-lock here to avoid ABBA deadlocks.
        // If a process try to start election and try to advance kernel pointer simultaneously, the situation may occur.
        //
        // try-promote            : LOCK(Control)  LOCK(CommandLog)
        // advance-kernel-pointer :       LOCK(CommandLog)   LOCK(Control)
        ensure!(cur_kern_index < self.ctrl_actor.try_read()?.commit_pointer);

        let process_index = cur_kern_index + 1;
        let e = self.command_log.get_entry(process_index).await?;
        self.command_log.kernel_pointer = process_index;

        let command = Command::deserialize(&e.command);
        let will_process = match command {
            Command::TermBarrier { .. } => true,
            Command::ClusterConfiguration { .. } => true,
            _ => false,
        };

        if will_process {
            Ok(Some(command_exec::KernelCommand {
                index: process_index,
                command: e.command,
                kernel_completion: self.command_log.kernel_completions.remove(&process_index),
            }))
        } else {
            Ok(None)
        }
    }
}
