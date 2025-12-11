use super::*;

mod command;
pub mod effect;
pub use command::Command;
mod response_cache;
use response_cache::ResponseCache;

pub struct Inner {
    /// Lock to serialize insertions to the log.
    write_sequencer: tokio::sync::Semaphore,
    storage: storage::LogStore,

    // Pointers in the log.
    // Invariant: commit_pointer >= kernel_pointer >= application_pointer >= snapshot_pointer
    pub commit_pointer: AtomicU64,
    kernel_pointer: AtomicU64,
    pub application_pointer: AtomicU64,
    pub snapshot_pointer: AtomicU64,

    app: App,
    response_cache: spin::Mutex<ResponseCache>,
    application_completions: spin::Mutex<BTreeMap<LogIndex, completion::ApplicationCompletion>>,
    kernel_completions: spin::Mutex<BTreeMap<LogIndex, completion::KernelCompletion>>,
}

#[derive(derive_more::Deref, Clone)]
pub struct StateMachine(pub Arc<Inner>);
impl StateMachine {
    pub fn new(storage: storage::LogStore, app: App) -> Self {
        let inner = Inner {
            app,
            write_sequencer: tokio::sync::Semaphore::new(1),
            commit_pointer: AtomicU64::new(0),
            kernel_pointer: AtomicU64::new(0),
            application_pointer: AtomicU64::new(0),
            snapshot_pointer: AtomicU64::new(0),
            storage,
            application_completions: spin::Mutex::new(BTreeMap::new()),
            kernel_completions: spin::Mutex::new(BTreeMap::new()),
            response_cache: spin::Mutex::new(ResponseCache::new()),
        };
        Self(inner.into())
    }
}

impl Inner {
    async fn insert_snapshot(&self, e: Entry) -> Result<()> {
        let new_snapshot_index = e.this_clock.index;

        self.storage.insert_entry(new_snapshot_index, e).await?;

        self.commit_pointer
            .fetch_max(new_snapshot_index - 1, Ordering::SeqCst);
        self.kernel_pointer
            .fetch_max(new_snapshot_index - 1, Ordering::SeqCst);
        self.application_pointer
            .fetch_max(new_snapshot_index - 1, Ordering::SeqCst);

        info!("inserted a new snapshot@{new_snapshot_index}");
        Ok(())
    }

    pub async fn get_log_head_index(&self) -> Result<LogIndex> {
        let head_log_index = self.storage.get_head_index().await?;
        Ok(head_log_index)
    }

    pub async fn get_log_last_index(&self) -> Result<LogIndex> {
        let last_log_index = self.storage.get_last_index().await?;
        Ok(last_log_index)
    }

    // This function won't return None because every caller of this function
    // doesn't care about the non-existence of the entry.
    pub async fn get_entry(&self, index: LogIndex) -> Result<Entry> {
        let entry = self.storage.get_entry(index).await?;
        ensure!(entry.is_some(), Error::EntryNotFound(index));
        Ok(entry.unwrap())
    }

    async fn insert_entry(&self, e: Entry) -> Result<()> {
        self.storage.insert_entry(e.this_clock.index, e).await?;
        Ok(())
    }

    /// Find the last last snapshot in `[, to]`.
    pub async fn find_last_snapshot_index(&self, to: LogIndex) -> Result<Option<LogIndex>> {
        for i in (1..=to).rev() {
            let e = self.get_entry(i).await?;
            match Command::deserialize(&e.command) {
                Command::Snapshot { .. } => return Ok(Some(i)),
                _ => {}
            }
        }
        Ok(None)
    }

    /// Find the last configuration in `[, to]`.
    pub async fn find_last_membership_index(&self, to: LogIndex) -> Result<Option<LogIndex>> {
        for i in (1..=to).rev() {
            let e = self.get_entry(i).await?;
            match Command::deserialize(&e.command) {
                Command::Snapshot { .. } => return Ok(Some(i)),
                Command::ClusterConfiguration { .. } => return Ok(Some(i)),
                _ => {}
            }
        }
        Ok(None)
    }

    /// Read the configuration at the given index.
    pub async fn try_read_membership(
        &self,
        index: LogIndex,
    ) -> Result<Option<HashSet<NodeAddress>>> {
        let e = self.get_entry(index).await?;
        match Command::deserialize(&e.command) {
            Command::Snapshot { membership } => Ok(Some(membership)),
            Command::ClusterConfiguration { membership } => Ok(Some(membership)),
            _ => Ok(None),
        }
    }

    pub fn register_completion(&self, index: LogIndex, completion: Completion) {
        match completion {
            Completion::Application(completion) => {
                self.application_completions
                    .lock()
                    .insert(index, completion);
            }
            Completion::Kernel(completion) => {
                self.kernel_completions.lock().insert(index, completion);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_fetch_max_monotonic() {
        let n = AtomicU64::new(50);
        n.fetch_max(10, Ordering::SeqCst);
        assert_eq!(n.load(Ordering::SeqCst), 50);
        n.fetch_max(100, Ordering::SeqCst);
        assert_eq!(n.load(Ordering::SeqCst), 100);
    }
}
