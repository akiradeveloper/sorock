use super::*;

pub enum TryInsertResult {
    Inserted,
    /// If the entry is already inserted then we can skip the insertion.
    SkippedInsertion,
    /// If the entry is inconsistent with the log then we should reject the entry.
    /// In this case, the leader should rewind the replication status to the follower.
    InconsistentInsertion {
        want: Clock,
        found: Clock,
    },
    LeapInsertion {
        want: Clock,
    },
}

pub struct Effect {
    pub state_machine: StateMachine,
    pub driver: RaftHandle,
}
impl Effect {
    pub async fn exec(self, entry: Entry, sender_id: NodeAddress) -> Result<TryInsertResult> {
        let _g = self.state_machine.write_sequencer.try_acquire()?;

        // If the entry is snapshot then we should insert this entry without consistency checks.
        // Old entries before the new snapshot will be garbage collected.
        match Command::deserialize(&entry.command) {
            Command::Snapshot { .. } => {
                let Clock {
                    term: _,
                    index: snapshot_index,
                } = entry.this_clock;
                warn!(
                    "log is too old. replicated a snapshot (idx={}) from leader",
                    snapshot_index
                );

                // Invariant: snapshot entry exists => snapshot exists
                if let Err(e) = self
                    .state_machine
                    .app
                    .fetch_snapshot(snapshot_index, sender_id.clone(), self.driver)
                    .await
                {
                    error!(
                        "could not fetch app snapshot (idx={}) from sender {}",
                        snapshot_index, sender_id,
                    );
                    return Err(e);
                }

                self.state_machine.insert_snapshot(entry).await?;

                return Ok(TryInsertResult::Inserted);
            }
            _ => {}
        }

        let Clock {
            term: _,
            index: prev_index,
        } = entry.prev_clock;
        if let Some(prev_clock) = self
            .state_machine
            .storage
            .get_entry(prev_index)
            .await?
            .map(|x| x.this_clock)
        {
            if prev_clock != entry.prev_clock {
                // consistency check failed.
                Ok(TryInsertResult::InconsistentInsertion {
                    want: entry.prev_clock,
                    found: prev_clock,
                })
            } else {
                let Clock {
                    term: _,
                    index: this_index,
                } = entry.this_clock;

                // optimization to skip actual insertion.
                if let Some(old_clock) = self
                    .state_machine
                    .storage
                    .get_entry(this_index)
                    .await?
                    .map(|e| e.this_clock)
                {
                    if old_clock == entry.this_clock {
                        // If there is a entry with the same term and index
                        // then the entry should be the same so we can skip insertion.
                        return Ok(TryInsertResult::SkippedInsertion);
                    }
                }

                self.state_machine.insert_entry(entry).await?;

                // discard [this_index, )
                self.state_machine
                    .application_completions
                    .lock()
                    .split_off(&this_index);
                self.state_machine
                    .kernel_completions
                    .lock()
                    .split_off(&this_index);

                Ok(TryInsertResult::Inserted)
            }
        } else {
            Ok(TryInsertResult::LeapInsertion {
                want: entry.prev_clock,
            })
        }
    }
}
