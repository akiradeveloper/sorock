use super::*;

impl CommandLog {
    pub async fn init(&mut self) -> Result<()> {
        let snapshot_index = match self.find_last_snapshot_index().await? {
            Some(x) => {
                info!("restore state: found snapshot_index={x}");
                x
            }
            None => {
                // If the log is new, insert an initial snapshot entry.
                // This ensures an invariant that there is always at least one snapshot in the log.
                let init_command = Command::serialize(Command::Snapshot {
                    membership: HashMap::new(),
                });
                let snapshot = Entry {
                    prev_clock: Clock { term: 0, index: 0 },
                    this_clock: Clock { term: 0, index: 1 },
                    command: init_command.clone(),
                };
                self.insert_entry(snapshot).await?;
                1
            }
        };

        self.kernel_pointer = snapshot_index - 1;
        self.app_pointer = snapshot_index - 1;
        self.snapshot_pointer = snapshot_index;

        // Normalize the log by deleting invalid entries.
        self.storage.delete_entries_before(snapshot_index).await?;
        let last_valid_index = self.traverse_valid_entries_from(snapshot_index).await?;
        self.storage.delete_entries_after(last_valid_index).await?;

        info!("restore state: snapshot_index={snapshot_index}, range=[{snapshot_index}, {last_valid_index}]");
        Ok(())
    }

    async fn traverse_valid_entries_from(&self, from: LogIndex) -> Result<LogIndex> {
        for i in from.. {
            let e1 = self.storage.get_entry(i).await?.unwrap();
            let Some(e2) = self.storage.get_entry(i + 1).await? else {
                return Ok(i);
            };

            if e1.this_clock != e2.prev_clock {
                return Ok(i);
            }
        }

        unreachable!()
    }

    /// Find the last snapshot in the log.
    async fn find_last_snapshot_index(&self) -> Result<Option<LogIndex>> {
        let to = self.storage.get_last_index().await?;
        for i in (1..=to).rev() {
            if let Some(e) = self.storage.get_entry(i).await? {
                match Command::deserialize(&e.command) {
                    Command::Snapshot { .. } => return Ok(Some(i)),
                    _ => {}
                }
            }
        }
        Ok(None)
    }
}
