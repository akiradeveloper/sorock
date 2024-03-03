use super::*;

pub enum TryInsertResult {
    Inserted,
    // If the entry is already inserted then we can skip the insertion.
    SkippedInsertion,
    // If the entry is inconsistent with the log then we should reject the entry.
    // In this case, the leader should rewind the replication status to the follower.
    InconsistencyDetected,
}

impl CommandLog {
    pub async fn append_new_entry(&self, command: Bytes, term: Option<Term>) -> Result<Index> {
        let _g = self.append_lock.lock().await;

        let cur_last_log_index = self.get_log_last_index().await?;
        let prev_clock = self.get_entry(cur_last_log_index).await?.this_clock;
        let append_index = cur_last_log_index + 1;
        let this_term = match term {
            Some(t) => t,
            None => prev_clock.term,
        };
        let this_clock = Clock {
            term: this_term,
            index: append_index,
        };
        let e = Entry {
            prev_clock,
            this_clock,
            command,
        };
        self.insert_entry(e).await?;

        Ok(append_index)
    }

    pub async fn try_insert_entry(
        &self,
        entry: Entry,
        sender_id: NodeId,
        driver: RaftDriver,
    ) -> Result<TryInsertResult> {
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
                    .app
                    .fetch_snapshot(snapshot_index, sender_id.clone(), driver)
                    .await
                {
                    error!(
                        "could not fetch app snapshot (idx={}) from sender {}",
                        snapshot_index, sender_id,
                    );
                    return Err(e);
                }

                self.insert_snapshot(entry).await?;

                self.commit_pointer
                    .store(snapshot_index - 1, Ordering::SeqCst);
                self.kern_pointer
                    .store(snapshot_index - 1, Ordering::SeqCst);
                self.user_pointer
                    .store(snapshot_index - 1, Ordering::SeqCst);

                return Ok(TryInsertResult::Inserted);
            }
            _ => {}
        }

        let Clock {
            term: _,
            index: prev_index,
        } = entry.prev_clock;
        if let Some(prev_clock) = self
            .storage
            .get_entry(prev_index)
            .await?
            .map(|x| x.this_clock)
        {
            if prev_clock != entry.prev_clock {
                // consistency check failed.
                Ok(TryInsertResult::InconsistencyDetected)
            } else {
                let Clock {
                    term: _,
                    index: this_index,
                } = entry.this_clock;

                // optimization to skip actual insertion.
                if let Some(old_clock) = self
                    .storage
                    .get_entry(this_index)
                    .await?
                    .map(|e| e.this_clock)
                {
                    if old_clock == entry.this_clock {
                        // If there is a entry with the same term and index
                        // then the entry should be the same so to skip insertion.
                        return Ok(TryInsertResult::SkippedInsertion);
                    }
                }

                self.insert_entry(entry).await?;

                // discard [this_index, )
                self.user_completions.lock().split_off(&this_index);
                self.kern_completions.lock().split_off(&this_index);

                Ok(TryInsertResult::Inserted)
            }
        } else {
            Ok(TryInsertResult::InconsistencyDetected)
        }
    }
}
