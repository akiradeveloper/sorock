use super::*;

pub struct Effect {
    pub state_mechine: StateMachine,
}
impl Effect {
    /// Append a new entry to the log.
    /// If `term` is None, then the term of the last entry is used.
    /// Otherwise, the given term is used to update the term of the last entry.
    pub async fn exec(&self, command: Bytes, term: Option<Term>) -> Result<LogIndex> {
        let _g = self.state_mechine.append_lock.lock().await;

        let cur_last_log_index = self.state_mechine.get_log_last_index().await?;
        let prev_clock = self
            .state_mechine
            .get_entry(cur_last_log_index)
            .await?
            .this_clock;
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
        self.state_mechine.insert_entry(e).await?;

        Ok(append_index)
    }
}
