use super::*;

impl CommandLog {


    /// Advance kernel process once.
    pub(crate) async fn advance_kern_process(&self, voter: Voter) -> Result<()> {
        let cur_kern_index = self.kern_pointer.load(Ordering::SeqCst);
        ensure!(cur_kern_index < self.commit_pointer.load(Ordering::SeqCst));

        let process_index = cur_kern_index + 1;
        let e = self.get_entry(process_index).await?;
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
                    voter.commit_safe_term(term);
                }
                Command::ClusterConfiguration { .. } => {}
                _ => {}
            }
            if let Some(kern_completion) = self.kern_completions.lock().remove(&process_index) {
                kern_completion.complete();
            }
        }

        self.kern_pointer.fetch_max(process_index, Ordering::SeqCst);

        Ok(())
    }
}
