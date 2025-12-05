use super::*;



impl CommandLog {
    /// Append a new entry to the log.
    /// If `term` is None, then the term of the last entry is used.
    /// Otherwise, the given term is used to update the term of the last entry.
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
}