use super::*;

mod consumer;
mod membership;
mod producer;
mod response_cache;
use response_cache::ResponseCache;

pub use producer::TryInsertResult;

pub struct Inner {
    /// Lock to serialize the append operation.
    append_lock: tokio::sync::Mutex<()>,
    storage: Box<dyn RaftLogStore>,

    // Pointers in the log.
    // Invariant: commit_pointer >= kern_pointer >= user_pointer >= snapshot_pointer
    pub commit_pointer: AtomicU64,
    kern_pointer: AtomicU64,
    pub user_pointer: AtomicU64,

    /// Lock entries in `[snapshot_index, user_application_index]`.
    snapshot_pointer: tokio::sync::RwLock<u64>,

    /// The index of the last membership.
    /// Unless `commit_index` >= membership_index`,
    /// new membership changes are not allowed to be queued.
    pub membership_pointer: AtomicU64,

    app: App,
    response_cache: spin::Mutex<ResponseCache>,
    user_completions: spin::Mutex<BTreeMap<Index, completion::UserCompletion>>,
    kern_completions: spin::Mutex<BTreeMap<Index, completion::KernCompletion>>,
}

#[derive(shrinkwraprs::Shrinkwrap, Clone)]
pub struct CommandLog(pub Arc<Inner>);
impl CommandLog {
    pub fn new(storage: impl RaftLogStore, app: App) -> Self {
        let inner = Inner {
            append_lock: tokio::sync::Mutex::new(()),
            commit_pointer: AtomicU64::new(0),
            kern_pointer: AtomicU64::new(0),
            user_pointer: AtomicU64::new(0),
            snapshot_pointer: tokio::sync::RwLock::new(0),
            membership_pointer: AtomicU64::new(0),
            storage: Box::new(storage),
            app,
            user_completions: spin::Mutex::new(BTreeMap::new()),
            kern_completions: spin::Mutex::new(BTreeMap::new()),
            response_cache: spin::Mutex::new(ResponseCache::new()),
        };
        Self(inner.into())
    }

    pub async fn restore_state(&self) -> Result<()> {
        let log_last_index = self.get_log_last_index().await?;
        let snapshot_index = match self.find_last_snapshot_index(log_last_index).await? {
            Some(x) => {
                info!("restore state: found snapshot_index={x}");
                x
            }
            None => {
                let init_command = Command::serialize(Command::Snapshot {
                    membership: HashSet::new(),
                });
                let snapshot = Entry {
                    prev_clock: Clock { term: 0, index: 0 },
                    this_clock: Clock { term: 0, index: 1 },
                    command: init_command.clone(),
                };
                self.insert_entry(snapshot).await?;
                1
            }
        };
        *self.snapshot_pointer.write().await = snapshot_index;

        self.commit_pointer
            .store(snapshot_index - 1, Ordering::SeqCst);
        self.kern_pointer
            .store(snapshot_index - 1, Ordering::SeqCst);
        self.user_pointer
            .store(snapshot_index - 1, Ordering::SeqCst);

        info!("restore state: snapshot_index={snapshot_index}");
        Ok(())
    }
}

impl Inner {
    /// Delete snapshots in `[, snapshot_index)`.
    pub async fn delete_old_snapshots(&self) -> Result<()> {
        let cur_snapshot_index = *self.snapshot_pointer.read().await;
        self.app.delete_snapshots_before(cur_snapshot_index).await?;
        Ok(())
    }

    /// Delete log entries in `[, snapshot_index)`.
    pub async fn delete_old_entries(&self) -> Result<()> {
        let cur_snapshot_index = *self.snapshot_pointer.read().await;
        self.storage
            .delete_entries_before(cur_snapshot_index)
            .await?;
        Ok(())
    }

    pub async fn insert_snapshot(&self, e: Entry) -> Result<()> {
        let new_snapshot_index = e.this_clock.index;

        self.storage.insert_entry(new_snapshot_index, e).await?;

        self.commit_pointer
            .fetch_max(new_snapshot_index - 1, Ordering::SeqCst);
        self.kern_pointer
            .fetch_max(new_snapshot_index - 1, Ordering::SeqCst);
        self.user_pointer
            .fetch_max(new_snapshot_index - 1, Ordering::SeqCst);

        info!("inserted a new snapshot@{new_snapshot_index}");
        Ok(())
    }

    pub async fn open_snapshot(&self, index: Index) -> Result<SnapshotStream> {
        let g_snapshot_pointer = self.snapshot_pointer.read().await;

        let cur_snapshot_index = *g_snapshot_pointer;
        ensure!(index == cur_snapshot_index);

        let st = self.app.open_snapshot(index).await?;
        Ok(st)
    }

    pub async fn get_snapshot_index(&self) -> Index {
        *self.snapshot_pointer.read().await
    }

    pub async fn get_log_head_index(&self) -> Result<Index> {
        let head_log_index = self.storage.get_head_index().await?;
        Ok(head_log_index)
    }

    pub async fn get_log_last_index(&self) -> Result<Index> {
        let last_log_index = self.storage.get_last_index().await?;
        Ok(last_log_index)
    }

    // This function won't return None because every caller of this function
    // doesn't care about the non-existence of the entry.
    pub async fn get_entry(&self, index: Index) -> Result<Entry> {
        let entry = self.storage.get_entry(index).await?;
        ensure!(entry.is_some(), Error::EntryNotFound(index));
        Ok(entry.unwrap())
    }

    pub async fn insert_entry(&self, e: Entry) -> Result<()> {
        self.storage.insert_entry(e.this_clock.index, e).await?;
        Ok(())
    }

    pub fn allow_queue_new_membership(&self) -> bool {
        self.commit_pointer.load(Ordering::SeqCst) >= self.membership_pointer.load(Ordering::SeqCst)
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
