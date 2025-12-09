use super::*;

pub struct Effect {
    pub state_machine: StateMachine,
    pub voter: Voter,
}

impl Effect {
    /// Advance kernel process once.
    pub async fn exec(self) -> Result<()> {
        let cur_kern_index = self.state_machine.kernel_pointer.load(Ordering::SeqCst);
        ensure!(cur_kern_index < self.state_machine.commit_pointer.load(Ordering::SeqCst));

        let process_index = cur_kern_index + 1;
        let e = self.state_machine.get_entry(process_index).await?;
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
                    self.voter.commit_safe_term(term);
                }
                Command::ClusterConfiguration { .. } => {}
                _ => {}
            }
            if let Some(kern_completion) = self
                .state_machine
                .kernel_completions
                .lock()
                .remove(&process_index)
            {
                kern_completion.complete();
            }
        }

        self.state_machine
            .kernel_pointer
            .fetch_max(process_index, Ordering::SeqCst);

        Ok(())
    }
}
