use super::*;

mod command;
pub mod effect;
pub use command::Command;
mod init;
mod response_cache;

pub struct CommandLog {
    storage: storage::LogStore,

    // Pointers in the log.
    // Invariant: kernel_pointer >= app_pointer >= snapshot_pointer
    pub kernel_pointer: u64,
    pub app_pointer: u64,
    pub snapshot_pointer: u64,

    app_completions: BTreeMap<LogIndex, completion::AppCompletion>,
    kernel_completions: BTreeMap<LogIndex, completion::KernelCompletion>,

    app: Arc<App>,
}

impl CommandLog {
    pub fn new(storage: storage::LogStore, app: Arc<App>) -> Self {
        Self {
            storage,
            kernel_pointer: 0,
            app_pointer: 0,
            snapshot_pointer: 0,
            app,
            app_completions: BTreeMap::new(),
            kernel_completions: BTreeMap::new(),
        }
    }

    async fn insert_snapshot(&mut self, e: Entry) -> Result<()> {
        let new_snapshot_index = e.this_clock.index;

        self.storage.insert_entry(new_snapshot_index, e).await?;

        // commit_pointer is not reset here because commit_pointer is autonomously restored
        // from the state of log replication.
        // In other words, even if we set commit_pointer 0 here, the correct commit_pointer
        // will be transferred from the leader again.

        self.kernel_pointer = new_snapshot_index - 1;
        self.app_pointer = new_snapshot_index - 1;
        self.snapshot_pointer = new_snapshot_index;

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

    async fn insert_entry(&mut self, e: Entry) -> Result<()> {
        self.storage.insert_entry(e.this_clock.index, e).await?;
        Ok(())
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
    ) -> Result<Option<HashMap<ServerAddress, bool>>> {
        let e = self.get_entry(index).await?;
        match Command::deserialize(&e.command) {
            Command::Snapshot { membership } => Ok(Some(membership)),
            Command::ClusterConfiguration { membership } => Ok(Some(membership)),
            _ => Ok(None),
        }
    }

    fn register_completion(&mut self, index: LogIndex, completion: Completion) {
        match completion {
            Completion::Application(completion) => {
                self.app_completions.insert(index, completion);
            }
            Completion::Kernel(completion) => {
                self.kernel_completions.insert(index, completion);
            }
        }
    }
}
