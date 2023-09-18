use super::*;

impl RaftProcess {
    async fn process_membership_change(&self, command: Bytes, index: Index) -> Result<()> {
        match Command::deserialize(&command) {
            Command::Snapshot { membership } => {
                self.peers
                    .set_membership(membership, index, Ref(self.voter.clone()))
                    .await?;
            }
            Command::ClusterConfiguration { membership } => {
                self.peers
                    .set_membership(membership, index, Ref(self.voter.clone()))
                    .await?;
            }
            _ => {}
        }
        Ok(())
    }

    pub async fn queue_new_entry(
        &self,
        command: Bytes,
        completion: Option<Completion>,
    ) -> Result<Index> {
        let cur_term = self.voter.allow_queue_entry().await?;

        let append_index = self
            .command_log
            .append_new_entry(command.clone(), completion, cur_term)
            .await?;

        self.process_membership_change(command, append_index)
            .await?;
        Ok(append_index)
    }

    pub async fn queue_received_entry(&self, mut req: LogStream) -> Result<bool> {
        let mut prev_clock = req.prev_clock;
        while let Some(cur) = req.entries.next().await {
            let entry = Entry {
                prev_clock,
                this_clock: cur.this_clock,
                command: cur.command,
            };
            let insert_index = entry.this_clock.index;
            let command = entry.command.clone();
            match self
                .command_log
                .try_insert_entry(entry, req.sender_id.clone(), self.driver.clone())
                .await?
            {
                command_log::TryInsertResult::Inserted => {
                    self.process_membership_change(command, insert_index)
                        .await?;
                }
                command_log::TryInsertResult::Skipped => {}
                command_log::TryInsertResult::Rejected => {
                    warn!("rejected append entry (clock={:?})", cur.this_clock);
                    return Ok(false);
                }
            }
            prev_clock = cur.this_clock;
        }
        Ok(true)
    }
}
