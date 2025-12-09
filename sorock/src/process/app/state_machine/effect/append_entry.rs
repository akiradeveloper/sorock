use super::*;

pub struct Effect {
    pub state_machine: StateMachine,
}
impl Effect {
    /// Append a new entry to the log.
    /// If `term` is None, then the term of the last entry is used.
    /// Otherwise, the given term is used to update the term of the last entry.
    pub async fn exec(&self, command: Bytes, term: Option<Term>) -> Result<LogIndex> {
        // I think using try- variant here will make client interactions unstable.
        let _g = self.state_machine.write_sequencer.acquire().await?;

        let cur_last_log_index = self.state_machine.get_log_last_index().await?;
        let prev_clock = self
            .state_machine
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
        self.state_machine.insert_entry(e).await?;

        Ok(append_index)
    }
}
