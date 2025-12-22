use super::*;

#[derive(Clone, Copy, Debug)]
pub struct Replication {
    /// The log entries `[0, match_index]` are replicated with this node.
    pub match_index: LogIndex,
    /// In the next replication, log entrires `[next_index, next_index + next_max_cnt)` will be sent.
    pub next_index: LogIndex,
    pub next_max_cnt: LogIndex,
}

impl Replication {
    pub fn new(init_next_index: LogIndex) -> Self {
        Self {
            match_index: 0,
            next_index: init_next_index,
            next_max_cnt: 1,
        }
    }
}
