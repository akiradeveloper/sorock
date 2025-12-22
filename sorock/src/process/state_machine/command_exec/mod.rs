use super::*;

mod app_exec;
mod kernel_exec;

pub use app_exec::AppExec;
pub use app_exec::{AppCommand, AppCommandBody};
pub use kernel_exec::KernelCommand;
pub use kernel_exec::KernelExec;

struct CommandWaitQueue<T> {
    q: BTreeMap<LogIndex, T>,
}

impl<T> CommandWaitQueue<T> {
    fn new() -> Self {
        Self { q: BTreeMap::new() }
    }

    fn insert(&mut self, index: LogIndex, v: T) {
        self.q.insert(index, v);
    }

    fn pop_next(&mut self) -> Option<(LogIndex, T)> {
        let Some((&next_index, _)) = self.q.iter().next() else {
            return None;
        };
        self.q.remove_entry(&next_index)
    }
}
