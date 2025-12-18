use super::*;

use anyhow::Result;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::collections::HashSet;
use tracing::{debug, error, info, warn};

use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;

mod storage;
pub use storage::RaftStorage;
mod api;
pub(super) use api::*;
use app::state_machine::StateMachine;
mod control;
use control::Control;
mod app;
use app::query_processing;
use app::state_machine;
use app::App;
use node::RaftHandle;
use state_machine::Command;

use app::completion;
mod kernel_message;
use completion::*;
mod thread;

/// Election term.
/// In Raft, only one leader can be elected per a term.
pub(super) type Term = u64;

/// Log index.
pub type LogIndex = u64;

/// Clock of log entry.
/// If two entries have the same clock, they should be the same entry.
/// It is like the hash of the git commit.
#[derive(Clone, Copy, Eq, Debug)]
pub(super) struct Clock {
    pub term: Term,
    pub index: LogIndex,
}
impl PartialEq for Clock {
    fn eq(&self, that: &Self) -> bool {
        self.term == that.term && self.index == that.index
    }
}

/// Log entry.
#[derive(Clone, Debug)]
struct Entry {
    prev_clock: Clock,
    this_clock: Clock,
    command: Bytes,
}

/// Ballot in election.
#[derive(Clone, Debug, PartialEq)]
struct Ballot {
    cur_term: Term,
    voted_for: Option<NodeAddress>,
}
impl Ballot {
    pub fn new() -> Self {
        Self {
            cur_term: 0,
            voted_for: None,
        }
    }
}

/// Snapshot is transferred as stream of bytes.
/// `SnapshotStream` is converted to gRPC streaming outside of the `RaftProcess`.
pub type SnapshotStream =
    std::pin::Pin<Box<dyn futures::stream::Stream<Item = anyhow::Result<Bytes>> + Send>>;

// This is only a marker that indicates the owner doesn't mutate the object.
// This is only to improve the readability.
// Compile-time or even runtime checking is more preferable.
#[derive(Deref, Clone)]
struct Read<T>(T);

/// `RaftApp` is an abstraction of state machine and snapshot store used by `RaftProcess`.
#[async_trait::async_trait]
pub trait RaftApp: Sync + Send + 'static {
    /// Apply read request to the application.
    /// Calling of this function should not change the state of the application.
    async fn process_read(&self, request: &[u8]) -> Result<Bytes>;

    /// Apply write request to the application.
    /// Calling of this function may change the state of the application.
    async fn process_write(&self, request: &[u8], entry_index: LogIndex) -> Result<Bytes>;

    /// Replace the state of the application with the snapshot.
    /// The snapshot is guaranteed to exist in the snapshot store.
    async fn install_snapshot(&self, snapshot_index: LogIndex) -> Result<()>;

    /// Save snapshot with index `snapshot_index` to the snapshot store.
    /// This function is called when the snapshot is fetched from the leader.
    async fn save_snapshot(&self, st: SnapshotStream, snapshot_index: LogIndex) -> Result<()>;

    /// Read existing snapshot with index `snapshot_index` from the snapshot store.
    /// This function is called when a follower requests a snapshot from the leader.
    async fn open_snapshot(&self, snapshot_index: LogIndex) -> Result<SnapshotStream>;

    /// Delete all the snapshots in `[,  i)` from the snapshot store.
    async fn delete_snapshots_before(&self, i: LogIndex) -> Result<()>;

    /// Get the index of the latest snapshot in the snapshot store.
    /// If the index is greater than the current snapshot entry index,
    /// it will replace the snapshot entry with the new one.
    async fn get_latest_snapshot(&self) -> Result<LogIndex>;
}

#[allow(dead_code)]
struct ThreadHandles {
    advance_kernel_handle: thread::ThreadHandle,
    advance_application_handle: thread::ThreadHandle,
    advance_snapshot_handle: thread::ThreadHandle,
    advance_commit_handle: thread::ThreadHandle,
    election_handle: thread::ThreadHandle,
    log_compaction_handle: thread::ThreadHandle,
    query_execution_handle: thread::ThreadHandle,
    query_queue_coordinator_handle: thread::ThreadHandle,
    snapshot_deleter_handle: thread::ThreadHandle,
    stepdown_handle: thread::ThreadHandle,
}

/// `RaftProcess` is a implementation of Raft process in `RaftNode`.
/// `RaftProcess` is agnostic to the I/O implementation and focuses on pure Raft algorithm.
pub struct RaftProcess {
    state_machine: StateMachine,
    ctrl: Control,
    query_queue: query_processing::PendingQueue,
    app: App,
    driver: node::RaftHandle,
    _thread_handles: ThreadHandles,

    queue_tx: thread::EventProducer<thread::QueueEvent>,
    replication_tx: thread::EventProducer<thread::ReplicationEvent>,
}

impl RaftProcess {
    pub async fn new(
        app: impl RaftApp,
        storage: &storage::RaftStorage,
        driver: node::RaftHandle,
    ) -> Result<Self> {
        let app = App::new(app);
        let (log_store, ballot_store) = storage.get(driver.shard_index)?;

        let query_queue = query_processing::PendingQueue::new();
        let exec_queue = query_processing::ReadyQueue::new(Read(app.clone()));

        let state_machine = StateMachine::new(log_store, app.clone());

        let (queue_tx, queue_rx) = thread::notify();
        let (replication_tx, replication_rx) = thread::notify();
        let (commit_tx, commit_rx) = thread::notify();
        let (kern_tx, kern_rx) = thread::notify();
        let (app_tx, app_rx) = thread::notify();

        let ctrl = Control::new(
            ballot_store,
            Read(state_machine.clone()),
            queue_rx.clone(),
            replication_tx.clone(),
            driver.clone(),
        );

        state_machine::effect::restore_state::Effect {
            state_machine: state_machine.clone(),
        }
        .exec()
        .await?;

        control::effect::restore_membership::Effect { ctrl: ctrl.clone() }
            .exec()
            .await?;

        let _thread_handles = ThreadHandles {
            advance_kernel_handle: thread::advance_kernel::new(
                state_machine.clone(),
                ctrl.clone(),
                commit_rx.clone(),
                kern_tx.clone(),
            ),
            advance_application_handle: thread::advance_application::new(
                state_machine.clone(),
                kern_rx.clone(),
                app_tx.clone(),
            ),
            advance_snapshot_handle: thread::advance_snapshot::new(state_machine.clone()),
            advance_commit_handle: thread::advance_commit::new(
                ctrl.clone(),
                replication_rx.clone(),
                commit_tx.clone(),
            ),
            election_handle: thread::election::new(ctrl.clone(), state_machine.clone()),
            log_compaction_handle: thread::delete_old_entries::new(state_machine.clone()),
            query_execution_handle: thread::query_execution::new(
                exec_queue.clone(),
                Read(state_machine.clone()),
                app_rx.clone(),
            ),
            query_queue_coordinator_handle: thread::query_queue_coordinator::new(
                query_queue.clone(),
                exec_queue,
                driver.clone(),
            ),
            snapshot_deleter_handle: thread::delete_old_snapshots::new(
                app.clone(),
                Read(state_machine.clone()),
            ),
            stepdown_handle: thread::stepdown::new(ctrl.clone()),
        };

        Ok(Self {
            state_machine,
            ctrl,
            query_queue,
            driver,
            app,
            _thread_handles,

            queue_tx,
            replication_tx,
        })
    }

    /// Process configuration change if the command contains configuration.
    /// Configuration should be applied as soon as it is inserted into the log because doing so
    /// guarantees that majority of the servers move to the configuration when the entry is committed.
    /// Without this property, servers may still be in some old configuration which may cause split-brain
    /// by electing two leaders in a single term which is not allowed in Raft.
    async fn process_configuration_command(&self, command: &[u8], index: LogIndex) -> Result<()> {
        let config0 = match Command::deserialize(command) {
            Command::Snapshot { membership } => Some(membership),
            Command::ClusterConfiguration { membership } => Some(membership),
            _ => None,
        };
        if let Some(config) = config0 {
            control::effect::set_membership::Effect {
                ctrl: self.ctrl.clone(),
            }
            .exec(config, index)
            .await?;
        }
        Ok(())
    }

    async fn queue_new_entry(&self, command: Bytes, completion: Completion) -> Result<LogIndex> {
        ensure!(self.ctrl.allow_queue_new_entry().await?);

        let append_index = state_machine::effect::append_entry::Effect {
            state_machine: self.state_machine.clone(),
        }
        .exec(command.clone(), None)
        .await?;

        self.state_machine
            .register_completion(append_index, completion);

        self.process_configuration_command(&command, append_index)
            .await?;

        self.queue_tx.push_event(thread::QueueEvent);
        self.replication_tx.push_event(thread::ReplicationEvent);

        Ok(append_index)
    }

    async fn queue_received_entries(&self, mut req: request::ReplicationStream) -> Result<u64> {
        let mut prev_clock = req.prev_clock;
        let mut n_inserted = 0;
        while let Some(Some(cur)) = req.entries.next().await {
            let entry = Entry {
                prev_clock,
                this_clock: cur.this_clock,
                command: cur.command,
            };
            let insert_index = entry.this_clock.index;
            let command = entry.command.clone();

            use state_machine::effect::try_insert::TryInsertResult;

            let insert_result = state_machine::effect::try_insert::Effect {
                state_machine: self.state_machine.clone(),
                driver: self.driver.clone(),
            }
            .exec(entry, req.sender_id.clone())
            .await?;

            match insert_result {
                TryInsertResult::Inserted => {
                    self.process_configuration_command(&command, insert_index)
                        .await?;
                }
                TryInsertResult::SkippedInsertion => {}
                TryInsertResult::InconsistentInsertion { want, found } => {
                    warn!("rejected append entry (clock={:?}) for inconsisntency (want:{want:?} != found:{found:?}", cur.this_clock);
                    break;
                }
                TryInsertResult::LeapInsertion { want } => {
                    debug!(
                        "rejected append entry (clock={:?}) for leap insertion (want={want:?})",
                        cur.this_clock
                    );
                    break;
                }
            }
            prev_clock = cur.this_clock;
            n_inserted += 1;
        }

        Ok(n_inserted)
    }

    /// Forming a new cluster with a single node is called "cluster bootstrapping".
    /// Raft algorith doesn't define adding node when the cluster is empty.
    /// We need to handle this special case.
    async fn bootstrap_cluster(&self) -> Result<()> {
        let mut membership = HashSet::new();
        membership.insert(self.driver.self_node_id());

        let command = Command::serialize(Command::ClusterConfiguration { membership });
        state_machine::effect::append_entry::Effect {
            state_machine: self.state_machine.clone(),
        }
        .exec(command.clone(), None)
        .await?;

        self.process_configuration_command(&command, 2).await?;

        // After this function is called
        // this server should immediately become the leader by self-vote and advance commit index.
        // Consequently, when initial install_snapshot is called this server is already the leader.
        let conn = self.driver.connect(self.driver.self_node_id());
        conn.send_timeout_now().await?;

        Ok(())
    }

    pub(super) async fn add_server(&self, req: request::AddServer) -> Result<()> {
        if self.ctrl.read_membership().is_empty() && req.server_id == self.driver.self_node_id() {
            self.bootstrap_cluster().await?;
        } else {
            let msg = kernel_message::KernelMessage::AddServer(req.server_id);
            let req = request::KernelRequest {
                message: msg.serialize(),
            };
            let conn = self.driver.connect(self.driver.self_node_id());
            conn.process_kernel_request(req).await?;
        }
        Ok(())
    }

    pub(super) async fn remove_server(&self, req: request::RemoveServer) -> Result<()> {
        let msg = kernel_message::KernelMessage::RemoveServer(req.server_id);
        let req = request::KernelRequest {
            message: msg.serialize(),
        };
        let conn = self.driver.connect(self.driver.self_node_id());
        conn.process_kernel_request(req).await?;
        Ok(())
    }

    pub(super) async fn send_replication_stream(
        &self,
        req: request::ReplicationStream,
    ) -> Result<response::ReplicationStream> {
        let n_inserted = self.queue_received_entries(req).await?;

        let resp = response::ReplicationStream {
            n_inserted,
            log_last_index: self.state_machine.get_log_last_index().await?,
        };
        Ok(resp)
    }

    pub(super) async fn process_kernel_request(&self, req: request::KernelRequest) -> Result<()> {
        let ballot = self.ctrl.read_ballot().await?;

        let Some(leader_id) = ballot.voted_for else {
            bail!(Error::LeaderUnknown)
        };

        if std::matches!(
            self.ctrl.read_election_state(),
            control::ElectionState::Leader
        ) {
            let (kern_completion, rx) = completion::prepare_kernel_completion();
            let command = match kernel_message::KernelMessage::deserialize(&req.message).unwrap() {
                kernel_message::KernelMessage::AddServer(id) => {
                    let mut membership = self.ctrl.read_membership();
                    membership.insert(id);
                    Command::ClusterConfiguration { membership }
                }
                kernel_message::KernelMessage::RemoveServer(id) => {
                    let mut membership = self.ctrl.read_membership();
                    membership.remove(&id);
                    Command::ClusterConfiguration { membership }
                }
            };
            ensure!(self.ctrl.allow_queue_new_membership());
            self.queue_new_entry(
                Command::serialize(command),
                Completion::Kernel(kern_completion),
            )
            .await?;

            rx.await?;
        } else {
            // Avoid looping.
            ensure!(self.driver.self_node_id() != leader_id);
            let conn = self.driver.connect(leader_id);
            conn.process_kernel_request(req).await?;
        }
        Ok(())
    }

    pub(super) async fn process_application_read_request(
        &self,
        req: request::ApplicationReadRequest,
    ) -> Result<Bytes> {
        let ballot = self.ctrl.read_ballot().await?;

        let Some(leader_id) = ballot.voted_for else {
            anyhow::bail!(Error::LeaderUnknown)
        };

        let will_process = req.read_locally
            || std::matches!(
                self.ctrl.read_election_state(),
                control::ElectionState::Leader
            );

        let resp = if will_process {
            let (app_completion, rx) = completion::prepare_application_completion();

            let query = query_processing::Query {
                message: req.message,
                app_completion,
            };
            self.query_queue.queue(query)?;

            rx.await?
        } else {
            // Avoid looping.
            ensure!(self.driver.self_node_id() != leader_id);
            let conn = self.driver.connect(leader_id);
            conn.process_application_read_request(req).await?
        };
        Ok(resp)
    }

    pub(super) async fn process_application_write_request(
        &self,
        req: request::ApplicationWriteRequest,
    ) -> Result<Bytes> {
        let ballot = self.ctrl.read_ballot().await?;

        let Some(leader_id) = ballot.voted_for else {
            bail!(Error::LeaderUnknown)
        };

        let resp = if std::matches!(
            self.ctrl.read_election_state(),
            control::ElectionState::Leader
        ) {
            let (app_completion, rx) = completion::prepare_application_completion();

            let command = Command::ExecuteRequest {
                message: &req.message,
                request_id: req.request_id,
            };

            self.queue_new_entry(
                Command::serialize(command),
                Completion::Application(app_completion),
            )
            .await?;

            rx.await?
        } else {
            // Avoid looping.
            ensure!(self.driver.self_node_id() != leader_id);
            let conn = self.driver.connect(leader_id);
            conn.process_application_write_request(req).await?
        };
        Ok(resp)
    }

    pub(super) async fn receive_heartbeat(
        &self,
        leader_id: NodeAddress,
        req: request::Heartbeat,
    ) -> Result<()> {
        let term = req.leader_term;
        let leader_commit = req.leader_commit_index;

        control::effect::receive_heartbeat::Effect {
            ctrl: self.ctrl.clone(),
        }
        .exec(leader_id, term, leader_commit)
        .await?;

        Ok(())
    }

    pub(super) async fn get_snapshot(&self, index: LogIndex) -> Result<SnapshotStream> {
        let cur_snapshot_index = self.state_machine.snapshot_pointer.load(Ordering::SeqCst);
        ensure!(index == cur_snapshot_index);
        let st = self.app.open_snapshot(index).await?;
        Ok(st)
    }

    pub(super) async fn send_timeout_now(&self) -> Result<()> {
        info!("received TimeoutNow. try to become a leader.");
        control::effect::try_promote::Effect {
            ctrl: self.ctrl.clone(),
            state_machine: self.state_machine.clone(),
        }
        .exec(true)
        .await?;
        Ok(())
    }

    pub(super) async fn request_vote(&self, req: request::RequestVote) -> Result<bool> {
        let candidate_term = req.vote_term;
        let candidate_id = req.candidate_id;
        let candidate_clock = req.candidate_clock;
        let force_vote = req.force_vote;
        let pre_vote = req.pre_vote;

        let vote_granted = control::effect::receive_vote_request::Effect {
            ctrl: self.ctrl.clone(),
            // state_machine: Read(self.state_machine.clone()),
        }
        .exec(
            candidate_term,
            candidate_id,
            candidate_clock,
            force_vote,
            pre_vote,
        )
        .await?;

        Ok(vote_granted)
    }

    pub(super) async fn get_log_state(&self) -> Result<response::LogState> {
        let out = response::LogState {
            head_index: self.state_machine.get_log_head_index().await?,
            last_index: self.state_machine.get_log_last_index().await?,
            snapshot_index: self.state_machine.snapshot_pointer.load(Ordering::SeqCst),
            application_index: self
                .state_machine
                .application_pointer
                .load(Ordering::SeqCst),
            commit_index: self.ctrl.commit_pointer.load(Ordering::SeqCst),
        };
        Ok(out)
    }

    pub(super) async fn get_membership(&self) -> Result<response::Membership> {
        let out = response::Membership {
            members: self.ctrl.read_membership(),
        };
        Ok(out)
    }

    pub async fn compare_term(&self, term: Term) -> Result<bool> {
        let cur_term = self.ctrl.read_ballot().await?.cur_term;
        Ok(term >= cur_term)
    }

    pub async fn issue_read_index(&self) -> Result<Option<LogIndex>> {
        let ballot = self.ctrl.read_ballot().await?;

        let Some(leader_id) = ballot.voted_for else {
            bail!(Error::LeaderUnknown)
        };

        if std::matches!(
            self.ctrl.read_election_state(),
            control::ElectionState::Leader
        ) {
            let read_index = self.ctrl.find_read_index().await?;
            Ok(read_index)
        } else {
            // Avoid looping.
            ensure!(self.driver.self_node_id() != leader_id);
            let conn = self.driver.connect(leader_id);
            let resp = conn.issue_read_index().await?;
            Ok(resp)
        }
    }
}
