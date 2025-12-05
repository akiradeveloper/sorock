use super::*;

pub struct Effect {
    pub command_log: CommandLog,
}

impl Effect {
    pub async fn exec(self) -> Result<()> {
        let log_last_index = self.command_log.get_log_last_index().await?;
        let snapshot_index = match self
            .command_log
            .find_last_snapshot_index(log_last_index)
            .await?
        {
            Some(x) => {
                info!("restore state: found snapshot_index={x}");
                x
            }
            None => {
                let init_command = Command::serialize(Command::Snapshot {
                    membership: HashSet::new(),
                });
                let snapshot = Entry {
                    prev_clock: Clock { term: 0, index: 0 },
                    this_clock: Clock { term: 0, index: 1 },
                    command: init_command.clone(),
                };
                self.command_log.insert_entry(snapshot).await?;
                1
            }
        };
        *self.command_log.snapshot_pointer.write().await = snapshot_index;

        self.command_log
            .commit_pointer
            .store(snapshot_index - 1, Ordering::SeqCst);
        self.command_log
            .kern_pointer
            .store(snapshot_index - 1, Ordering::SeqCst);
        self.command_log
            .user_pointer
            .store(snapshot_index - 1, Ordering::SeqCst);

        info!("restore state: snapshot_index={snapshot_index}");
        Ok(())
    }
}
