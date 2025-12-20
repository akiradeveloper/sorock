use super::*;

pub struct Effect<'a> {
    pub command_log: &'a mut CommandLog,
}
impl Effect<'_> {
    /// Append a new entry to the log.
    ///
    /// If `term` is None, the term of the last entry is used.
    /// Otherwise, the given term is used.
    ///
    /// If `completion` is Some, register the completion callback for the appended entry.
    pub async fn exec(
        self,
        command: Bytes,
        term: Option<Term>,
        completion: Option<Completion>,
    ) -> Result<LogIndex> {
        let cur_last_log_index = self.command_log.get_log_last_index().await?;
        let prev_clock = self
            .command_log
            .get_entry(cur_last_log_index)
            .await?
            .this_clock;
        let append_index = cur_last_log_index + 1;

        let this_clock = {
            let this_term = match term {
                Some(t) => t,
                None => prev_clock.term,
            };
            Clock {
                term: this_term,
                index: append_index,
            }
        };

        let e = Entry {
            prev_clock,
            this_clock,
            command,
        };
        self.command_log.insert_entry(e).await?;

        if let Some(completion) = completion {
            self.command_log
                .register_completion(append_index, completion);
        }

        Ok(append_index)
    }
}
