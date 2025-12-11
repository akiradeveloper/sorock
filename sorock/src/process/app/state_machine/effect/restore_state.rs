use super::*;

pub struct Effect {
    pub state_machine: StateMachine,
}

impl Effect {
    pub async fn exec(self) -> Result<()> {
        let log_last_index = self.state_machine.get_log_last_index().await?;
        let snapshot_index = match self
            .state_machine
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
                self.state_machine.insert_entry(snapshot).await?;
                1
            }
        };

        self.state_machine
            .commit_pointer
            .fetch_max(snapshot_index - 1, Ordering::SeqCst);
        self.state_machine
            .kernel_pointer
            .store(snapshot_index - 1, Ordering::SeqCst);
        self.state_machine
            .application_pointer
            .store(snapshot_index - 1, Ordering::SeqCst);
        self.state_machine
            .snapshot_pointer
            .store(snapshot_index, Ordering::SeqCst);

        info!("restore state: snapshot_index={snapshot_index}");
        Ok(())
    }
}
