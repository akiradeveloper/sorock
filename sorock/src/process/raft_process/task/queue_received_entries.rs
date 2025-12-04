use super::*;

pub struct Task {
    pub command_log: CommandLog,
    pub voter: Ref<Voter>,
    pub peers: PeerSvc,
    pub app: App,
    pub driver: RaftDriver,
}
impl Task {
    pub async fn exec(
        &self,
        mut req: request::ReplicationStream,
    ) -> Result<u64> {
        let mut prev_clock = req.prev_clock;
        let mut n_inserted = 0;
        while let Some(Some(cur)) = req.entries.next().await {
            let entry = Entry {
                prev_clock,
                this_clock: cur.this_clock,
                command: cur.command,
            };
            let insert_index = entry.this_clock.index;
            let command = entry.command.clone();

            use command_log::TryInsertResult;
            match self
                .command_log
                .try_insert_entry(entry, req.sender_id.clone(), self.driver.clone(), self.app.clone())
                .await?
            {
                TryInsertResult::Inserted => {
                    process_configuration_command::Task {
                        peers: self.peers.clone(),
                        voter: self.voter.clone(),
                    }.exec(&command, insert_index).await?;
                }
                TryInsertResult::SkippedInsertion => {}
                TryInsertResult::InconsistentInsertion { want, found } => {
                    warn!("rejected append entry (clock={:?}) for inconsisntency (want:{want:?} != found:{found:?}", cur.this_clock);
                    break;
                }
                TryInsertResult::LeapInsertion { want } => {
                    debug!(
                        "rejected append entry (clock={:?}) for leap insertion (want={want:?})",
                        cur.this_clock
                    );
                    break;
                }
            }
            prev_clock = cur.this_clock;
            n_inserted += 1;
        }

        Ok(n_inserted)
    }
}