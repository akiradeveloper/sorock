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
    pub snapshot_pointer: AtomicU64,

    /// Lock entries in `[snapshot_index, user_application_index]`.
    snapshot_lock: tokio::sync::RwLock<()>,

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
            snapshot_pointer: AtomicU64::new(0),
            commit_pointer: AtomicU64::new(0),
            kern_pointer: AtomicU64::new(0),
            user_pointer: AtomicU64::new(0),
            membership_pointer: AtomicU64::new(0),
            storage: Box::new(storage),
            app,
            snapshot_lock: tokio::sync::RwLock::new(()),
            user_completions: spin::Mutex::new(BTreeMap::new()),
            kern_completions: spin::Mutex::new(BTreeMap::new()),
            response_cache: spin::Mutex::new(ResponseCache::new()),
        };
        Self(inner.into())
    }

    pub async fn restore_state(&self) -> Result<()> {
        let log_last_index = self.get_log_last_index().await?;
        let snapshot_index = match self.find_last_snapshot_index(log_last_index).await? {
            Some(x) => x,
            None => 0,
        };
        let start_index = if snapshot_index == 0 {
            0
        } else {
            // After reboot, we forgot the `commit_index` in the previous run.
            // Conservatively set `commit_index` to right before the `snapshot_index`
            // so we can restart from installing the snapshot.
            snapshot_index - 1
        };
        self.commit_pointer.store(start_index, Ordering::SeqCst);
        self.kern_pointer.store(start_index, Ordering::SeqCst);
        self.user_pointer.store(start_index, Ordering::SeqCst);

        Ok(())
    }
}

impl Inner {
    /// Delete snapshots in `[, snapshot_index)`.
    pub async fn delete_old_snapshots(&self) -> Result<()> {
        let cur_snapshot_index = self.snapshot_pointer.load(Ordering::Relaxed);
        self.app.delete_snapshots_before(cur_snapshot_index).await?;
        Ok(())
    }

    /// Delete log entries in `[, snapshot_index)`.
    pub async fn delete_old_entries(&self) -> Result<()> {
        let cur_snapshot_index = self.snapshot_pointer.load(Ordering::Relaxed);
        self.storage
            .delete_entries_before(cur_snapshot_index)
            .await?;
        Ok(())
    }

    pub async fn insert_snapshot(&self, e: Entry) -> Result<()> {
        let _g = self.snapshot_lock.write().await;

        let cur_snapshot_index = self.snapshot_pointer.load(Ordering::SeqCst);
        let new_snapshot_index = e.this_clock.index;

        // If owned snapshot is newer than the coming snapshot,
        // the leader should be able to send the subsequent non-snapshot entries.
        ensure!(new_snapshot_index >= cur_snapshot_index);

        // If the same snapshot already exists, we can skip the insertion.
        if new_snapshot_index == cur_snapshot_index {
            return Ok(());
        }

        self.storage.insert_entry(new_snapshot_index, e).await?;

        self.snapshot_pointer
            .store(new_snapshot_index, Ordering::SeqCst);

        info!("inserted a new snapshot@{new_snapshot_index} (prev={cur_snapshot_index})");
        Ok(())
    }

    pub async fn open_snapshot(&self, index: Index) -> Result<SnapshotStream> {
        let _g = self.snapshot_lock.read().await;

        let cur_snapshot_index = self.snapshot_pointer.load(Ordering::SeqCst);
        ensure!(index == cur_snapshot_index);

        let st = self.app.open_snapshot(index).await?;
        Ok(st)
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
