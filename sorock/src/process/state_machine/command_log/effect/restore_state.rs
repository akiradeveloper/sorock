use super::*;

pub struct Effect<'a> {
    pub command_log: &'a mut CommandLog,
}

impl Effect<'_> {
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
                // If the log is new, insert an initial snapshot entry.
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

        self.command_log.kernel_pointer = snapshot_index - 1;
        self.command_log.application_pointer = snapshot_index - 1;
        self.command_log.snapshot_pointer = snapshot_index;

        info!("restore state: snapshot_index={snapshot_index}");
        Ok(())
    }
}
