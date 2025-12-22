use super::*;

pub struct Effect<'a> {
    pub progress: &'a mut Replication,
    pub ctrl: &'a Control,
}

impl Effect<'_> {
    fn command_log(&self) -> &Read<Actor<CommandLog>> {
        &self.ctrl.command_log
    }

    /// Prepare a replication stream from the log entries `[l, r)`.
    async fn prepare_replication_stream(
        selfid: NodeAddress,
        term: Term,
        command_log: Read<Actor<CommandLog>>,
        l: LogIndex,
        r: LogIndex,
    ) -> Result<request::ReplicationStream> {
        let head = command_log.read().await.get_entry(l).await?;

        let st = async_stream::stream! {
            for idx in l..r {
                let x = command_log.read().await.get_entry(idx).await;
                let e = match x {
                    Ok(x) => Some(request::ReplicationStreamElem {
                        this_clock: x.this_clock,
                        command: x.command,
                    }),
                    Err(_) => None,
                };
                yield e;
            }
        };

        Ok(request::ReplicationStream {
            sender_id: selfid,
            sender_term: term,
            prev_clock: head.prev_clock,
            entries: Box::pin(st),
        })
    }

    pub async fn exec(self, follower_id: NodeAddress) -> Result<()> {
        let cur_term = self.ctrl.read_ballot().await?.cur_term;

        let cur_progress = *self.progress;

        let old_progress = cur_progress;
        let cur_last_log_index = self.command_log().read().await.get_log_last_index().await?;

        // More entries to send?
        ensure!(old_progress.next_index <= cur_last_log_index);

        // The entries to be sent may be deleted due to a previous compaction.
        // In this case, replication will reset from the current snapshot index.
        let cur_snapshot_index = self.command_log().read().await.snapshot_pointer;
        if old_progress.next_index < cur_snapshot_index {
            warn!(
                "entry not found at next_index (idx={}) for {}",
                old_progress.next_index, follower_id,
            );
            let new_progress = Replication::new(cur_snapshot_index);
            *self.progress = new_progress;
            return Ok(());
        }

        let n_max_possible = cur_last_log_index - old_progress.next_index + 1;
        let n = std::cmp::min(old_progress.next_max_cnt, n_max_possible);
        ensure!(n >= 1);

        let out_stream = Self::prepare_replication_stream(
            self.ctrl.driver.self_node_id(),
            cur_term,
            self.command_log().clone(),
            old_progress.next_index,
            old_progress.next_index + n,
        )
        .await?;

        let conn = self.ctrl.driver.connect(follower_id.clone());

        // If the follower is unable to respond for some internal reasons,
        // we shouldn't repeat request otherwise the situation would be worse.
        let resp = conn.send_replication_stream(out_stream).await?;

        let new_progress = match resp {
            response::ReplicationStream {
                n_inserted: 0,
                log_last_index: last_log_index,
            } => Replication {
                match_index: old_progress.match_index,
                next_index: std::cmp::min(old_progress.next_index - 1, last_log_index + 1),
                next_max_cnt: 1,
            },
            response::ReplicationStream { n_inserted, .. } => Replication {
                match_index: old_progress.next_index + n_inserted - 1,
                next_index: old_progress.next_index + n_inserted,
                // If all entries are successfully inserted, then it is safe to double
                // the replication width for quick replication.
                next_max_cnt: if n_inserted == n { n * 2 } else { n },
            },
        };

        *self.progress = new_progress;

        Ok(())
    }
}
