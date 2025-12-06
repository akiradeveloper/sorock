use super::*;

pub struct Effect {
    pub state_mechine: StateMachine,
}

impl Effect {
    pub async fn exec(self) -> Result<()> {
        let log_last_index = self.state_mechine.get_log_last_index().await?;
        let snapshot_index = match self
            .state_mechine
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
                self.state_mechine.insert_entry(snapshot).await?;
                1
            }
        };
        *self.state_mechine.snapshot_pointer.write().await = snapshot_index;

        self.state_mechine
            .commit_pointer
            .store(snapshot_index - 1, Ordering::SeqCst);
        self.state_mechine
            .kernel_pointer
            .store(snapshot_index - 1, Ordering::SeqCst);
        self.state_mechine
            .application_pointer
            .store(snapshot_index - 1, Ordering::SeqCst);

        info!("restore state: snapshot_index={snapshot_index}");
        Ok(())
    }
}
