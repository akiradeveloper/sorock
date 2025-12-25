use super::*;

use anyhow::Result;
use std::collections::BTreeMap;
use std::collections::HashMap;
use tokio::sync::{Mutex, RwLock};
use tracing::{debug, error, info, warn};

mod storage;
pub use storage::RaftStorage;
mod api;
pub(super) use api::*;
mod control;
use control::Control;
use node::RaftIO;

mod state_machine;
use command_log::{Command, CommandLog};
use state_machine::command_exec::{AppExec, KernelExec};
use state_machine::command_log;
use state_machine::completion;
use state_machine::query_queue;
use state_machine::App;

mod kernel_message;
use completion::*;
mod thread;
use thread::utils::*;

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
    voted_for: Option<ServerAddress>,
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

pub type Actor<T> = Arc<RwLock<T>>;

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
    async fn open_snapshot(&self, snapshot_index: LogIndex) -> Result<Option<SnapshotStream>>;

    /// Delete all the snapshots in `[,  i)` from the snapshot store.
    async fn delete_snapshots_before(&self, i: LogIndex) -> Result<()>;

    /// Get the index of the latest snapshot in the snapshot store.
    /// If the index is greater than the current snapshot entry index,
    /// it will replace the snapshot entry with the new one.
    async fn get_latest_snapshot(&self) -> Result<LogIndex>;
}

#[allow(dead_code)]
struct ThreadHandles {
    prepare_kernel_exec_handle: ThreadHandle,
    prepare_app_exec_handle: ThreadHandle,
    run_kernel_exec_handle: ThreadHandle,
    run_app_exec_handle: ThreadHandle,
    advance_snapshot_handle: ThreadHandle,
    advance_commit_handle: ThreadHandle,
    election_handle: ThreadHandle,
    log_compaction_handle: ThreadHandle,
    run_query_exec_handle: ThreadHandle,
    prepare_query_exec_handle: ThreadHandle,
    snapshot_deleter_handle: ThreadHandle,
    stepdown_handle: ThreadHandle,
}

struct Gateway {
    command_log_actor: Actor<CommandLog>,
    ctrl_actor: Actor<Control>,
    queue_evt_tx: EventNotifier<QueueEvent>,
}

impl Gateway {
    /// Process configuration change if the command contains configuration.
    /// Configuration should be applied as soon as it is inserted into the log because doing so
    /// guarantees that majority of the servers move to the configuration when the entry is committed.
    /// Without this property, servers may still be in some old configuration which may cause split-brain
    /// by electing two leaders in a single term which is not allowed in Raft.
    async fn process_configuration_command(
        &mut self,
        command: &[u8],
        index: LogIndex,
    ) -> Result<()> {
        let config0 = match Command::deserialize(command) {
            Command::Snapshot { membership } => Some(membership),
            Command::ClusterConfiguration { membership } => Some(membership),
            _ => None,
        };
        if let Some(config) = config0 {
            control::effect::set_membership::Effect {
                ctrl: &mut *self.ctrl_actor.write().await,
                ctrl_actor: self.ctrl_actor.clone(),
            }
            .exec(config, index)
            .await?;
        }
        Ok(())
    }

    async fn queue_new_entry(&mut self, command: Bytes, completion: Completion) -> Result<()> {
        ensure!(self.ctrl_actor.read().await.allow_queue_new_entry().await?);

        let append_index = command_log::effect::append_entry::Effect {
            command_log: &mut *self.command_log_actor.write().await,
        }
        .exec(command.clone(), None, Some(completion))
        .await?;

        self.process_configuration_command(&command, append_index)
            .await?;

        self.queue_evt_tx.push_event(QueueEvent);

        Ok(())
    }

    async fn queue_received_entries(&mut self, mut req: request::ReplicationStream) -> Result<u64> {
        let cur_term = self.ctrl_actor.read().await.read_ballot().await?.cur_term;
        ensure!(
            cur_term <= req.sender_term,
            "received replication stream from stale leader (term={} < cur_term={})",
            req.sender_term,
            cur_term
        );

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

            use command_log::effect::try_insert::TryInsertResult;

            let insert_result = command_log::effect::try_insert::Effect {
                command_log: &mut *self.command_log_actor.write().await,
            }
            .exec(entry, req.sender_id.clone())
            .await?;

            match insert_result {
                TryInsertResult::Inserted => {
                    self.process_configuration_command(&command, insert_index)
                        .await?;
                }
                TryInsertResult::SkippedInsertion => {}
                TryInsertResult::InconsistentInsertion {
                    prev_expected: want,
                    prev_actual: found,
                } => {
                    warn!("rejected append entry (clock={:?}) for inconsisntency (want:{want:?} != found:{found:?}", cur.this_clock);
                    break;
                }
                TryInsertResult::LeapInsertion {
                    prev_expected: want,
                } => {
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
}

/// `RaftProcess` is a implementation of Raft process in `RaftNode`.
/// `RaftProcess` is agnostic to the I/O implementation and focuses on pure Raft algorithm.
pub struct RaftProcess {
    command_log_actor: Actor<CommandLog>,
    ctrl_actor: Actor<Control>,
    query_queue: query_queue::QueryQueue,
    app: Arc<App>,
    io: node::RaftIO,
    _thread_handles: ThreadHandles,

    gateway: Arc<Mutex<Gateway>>,
}

impl RaftProcess {
    pub async fn new(
        app: impl RaftApp,
        storage: &storage::RaftStorage,
        io: node::RaftIO,
    ) -> Result<Self> {
        let (queue_evt_tx, queue_evt_rx) = thread::notify();
        let (replication_evt_tx, replication_evt_rx) = thread::notify();
        let (commit_evt_tx, commit_evt_rx) = thread::notify();
        let (kernel_queue_evt_tx, kernel_queue_evt_rx) = thread::notify();
        let (app_queue_evt_tx, app_queue_evt_rx) = thread::notify();
        let (applied_evt_tx, applied_evt_rx) = thread::notify();

        let app = Arc::new(App::new(app, io.clone()));
        let (log_store, ballot_store) = storage.get(io.shard_id)?;

        let query_queue = Arc::new(parking_lot::Mutex::new(query_queue::QueryQueueRaw::new()));
        let query_exec_actor = Arc::new(RwLock::new(query_queue::QueryExec::new(app.clone())));

        let mut command_log = CommandLog::new(log_store, app.clone());
        command_log.init().await?;

        let command_log_actor = Arc::new(RwLock::new(command_log));

        let ctrl = Control::new(
            ballot_store,
            command_log_actor.clone(),
            queue_evt_rx.clone(),
            replication_evt_tx.clone(),
            io.clone(),
        );
        let ctrl_actor = Arc::new(RwLock::new(ctrl));
        ctrl_actor.write().await.init(ctrl_actor.clone()).await?;

        let app_exec_actor = Arc::new(RwLock::new(AppExec::new(
            app.clone(),
            command_log_actor.clone(),
        )));

        let kernel_exec_actor = Arc::new(RwLock::new(KernelExec::new(ctrl_actor.clone())));

        let _thread_handles = ThreadHandles {
            prepare_kernel_exec_handle: thread::prepare_kernel_exec::run(
                command_log_actor.clone(),
                ctrl_actor.clone(),
                kernel_exec_actor.clone(),
                commit_evt_rx.clone(),
                kernel_queue_evt_tx.clone(),
            ),
            run_kernel_exec_handle: thread::run_kernel_exec::run(
                kernel_exec_actor.clone(),
                kernel_queue_evt_rx.clone(),
            ),
            prepare_app_exec_handle: thread::prepare_app_exec::run(
                command_log_actor.clone(),
                app_exec_actor.clone(),
                kernel_queue_evt_rx.clone(),
                app_queue_evt_tx.clone(),
            ),
            run_app_exec_handle: thread::run_app_exec::run(
                app_exec_actor.clone(),
                app_queue_evt_rx.clone(),
                applied_evt_tx.clone(),
            ),
            prepare_query_exec_handle: thread::prepare_query_exec::run(
                query_queue.clone(),
                query_exec_actor.clone(),
                io.clone(),
            ),
            run_query_exec_handle: thread::run_query_exec::run(
                query_exec_actor,
                app_exec_actor.clone(),
                applied_evt_rx.clone(),
            ),
            advance_snapshot_handle: thread::advance_snapshot::run(command_log_actor.clone()),
            advance_commit_handle: control::thread::advance_commit::run(
                ctrl_actor.clone(),
                replication_evt_rx.clone(),
                commit_evt_tx.clone(),
            ),
            election_handle: control::thread::election::run(
                ctrl_actor.clone(),
                command_log_actor.clone(),
            ),
            log_compaction_handle: thread::delete_old_entries::run(command_log_actor.clone()),
            snapshot_deleter_handle: thread::delete_old_snapshots::run(
                app.clone(),
                command_log_actor.clone(),
            ),
            stepdown_handle: control::thread::stepdown::run(ctrl_actor.clone()),
        };

        let gateway = Arc::new(Mutex::new(Gateway {
            command_log_actor: command_log_actor.clone(),
            ctrl_actor: ctrl_actor.clone(),
            queue_evt_tx: queue_evt_tx.clone(),
        }));

        Ok(Self {
            command_log_actor,
            ctrl_actor,
            query_queue,
            io,
            app,
            _thread_handles,

            gateway,
        })
    }

    /// Forming a new cluster with a single node is called "cluster bootstrapping".
    /// Raft algorith doesn't define adding node when the cluster is empty.
    /// We need to handle this special case.
    async fn bootstrap_cluster(&self, as_voter: bool) -> Result<()> {
        let mut membership = HashMap::new();
        membership.insert(self.io.local_server_id.clone(), as_voter);

        let command = Command::serialize(Command::ClusterConfiguration {
            membership: membership.clone(),
        });
        command_log::effect::append_entry::Effect {
            command_log: &mut *self.command_log_actor.write().await,
        }
        .exec(command.clone(), None, None)
        .await?;

        control::effect::set_membership::Effect {
            ctrl: &mut *self.ctrl_actor.write().await,
            ctrl_actor: self.ctrl_actor.clone(),
        }
        .exec(membership, 2)
        .await?;

        // After this function is called
        // this server should immediately become the leader by self-vote and advance commit index.
        // Consequently, when initial install_snapshot is called this server is already the leader.
        let conn = self.io.connect(&self.io.local_server_id);
        conn.send_timeout_now().await.ok();

        Ok(())
    }

    pub(super) async fn add_server(&self, req: request::AddServer) -> Result<()> {
        if self.ctrl_actor.read().await.read_membership().is_empty()
            && req.server_id == self.io.local_server_id
        {
            self.bootstrap_cluster(req.as_voter).await?;
        } else {
            let msg = kernel_message::KernelMessage::AddServer(req.server_id, req.as_voter);
            let req = request::KernelRequest {
                message: msg.serialize(),
            };
            let conn = self.io.connect(&self.io.local_server_id);
            conn.process_kernel_request(req).await?;
        }
        Ok(())
    }

    pub(super) async fn remove_server(&self, req: request::RemoveServer) -> Result<()> {
        let msg = kernel_message::KernelMessage::RemoveServer(req.server_id);
        let req = request::KernelRequest {
            message: msg.serialize(),
        };
        let conn = self.io.connect(&self.io.local_server_id);
        conn.process_kernel_request(req).await?;
        Ok(())
    }

    pub(super) async fn send_replication_stream(
        &self,
        req: request::ReplicationStream,
    ) -> Result<response::ReplicationStream> {
        let n_inserted = self
            .gateway
            .lock()
            .await
            .queue_received_entries(req)
            .await?;

        let resp = response::ReplicationStream {
            n_inserted,
            log_last_index: self.command_log_actor.read().await.tail_pointer,
        };
        Ok(resp)
    }

    pub(super) async fn process_kernel_request(&self, req: request::KernelRequest) -> Result<()> {
        let ballot = self.ctrl_actor.read().await.read_ballot().await?;

        let Some(leader_id) = ballot.voted_for else {
            bail!(Error::LeaderUnknown)
        };

        if self.ctrl_actor.read().await.is_leader() {
            let (kern_completion, rx) = completion::prepare_kernel_completion();
            let command = match kernel_message::KernelMessage::deserialize(&req.message).unwrap() {
                kernel_message::KernelMessage::AddServer(id, as_voter) => {
                    ensure!(self.ctrl_actor.read().await.allow_queue_new_membership());

                    let mut membership = self.ctrl_actor.read().await.read_membership();
                    membership.insert(id, as_voter);
                    Command::ClusterConfiguration { membership }
                }
                kernel_message::KernelMessage::RemoveServer(id) => {
                    ensure!(self.ctrl_actor.read().await.allow_queue_new_membership());

                    let mut membership = self.ctrl_actor.read().await.read_membership();
                    membership.remove(&id);
                    Command::ClusterConfiguration { membership }
                }
            };
            self.gateway
                .lock()
                .await
                .queue_new_entry(
                    Command::serialize(command),
                    Completion::Kernel(kern_completion),
                )
                .await?;

            rx.await?;
        } else {
            // Avoid looping.
            ensure!(self.io.local_server_id != leader_id);
            let conn = self.io.connect(&leader_id);
            conn.process_kernel_request(req).await?;
        }
        Ok(())
    }

    pub(super) async fn process_app_read_request(
        &self,
        req: request::AppReadRequest,
    ) -> Result<Bytes> {
        let (app_completion, rx) = completion::prepare_app_completion();

        let query = query_queue::Query {
            message: req.message,
            app_completion,
        };
        self.query_queue.lock().queue(query)?;

        let resp = rx.await?;
        Ok(resp)
    }

    pub(super) async fn process_app_write_request(
        &self,
        req: request::AppWriteRequest,
    ) -> Result<Bytes> {
        let ballot = self.ctrl_actor.read().await.read_ballot().await?;

        let Some(leader_id) = ballot.voted_for else {
            bail!(Error::LeaderUnknown)
        };

        let resp = if self.ctrl_actor.read().await.is_leader() {
            let (app_completion, rx) = completion::prepare_app_completion();

            let command = Command::ExecuteWriteRequest {
                message: &req.message,
                request_id: req.request_id,
            };

            self.gateway
                .lock()
                .await
                .queue_new_entry(
                    Command::serialize(command),
                    Completion::Application(app_completion),
                )
                .await?;

            rx.await?
        } else {
            // Avoid looping.
            ensure!(self.io.local_server_id != leader_id);
            let conn = self.io.connect(&leader_id);
            conn.process_application_write_request(req).await?
        };
        Ok(resp)
    }

    pub(super) async fn receive_heartbeat(
        &self,
        leader_id: ServerAddress,
        req: request::Heartbeat,
    ) -> Result<()> {
        let term = req.sender_term;
        let leader_commit = req.sender_commit_index;

        control::effect::receive_heartbeat::Effect {
            ctrl: &mut *self.ctrl_actor.write().await,
        }
        .exec(leader_id, term, leader_commit)
        .await?;

        Ok(())
    }

    pub(super) async fn get_snapshot(&self, index: LogIndex) -> Result<SnapshotStream> {
        let st = self
            .app
            .open_snapshot(index)
            .await?
            .context(Error::SnapshotNotFound(index))?;
        Ok(st)
    }

    pub(super) async fn send_timeout_now(&self) -> Result<()> {
        info!("received TimeoutNow. try to become a leader.");
        control::effect::try_promote::Effect {
            ctrl: &mut *self.ctrl_actor.write().await,
            command_log: self.command_log_actor.clone(),
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
            // We must take try lock here to avoid deadlock:
            // Two processes time out and try to become a leader simultaneously.
            // Both of them send vote requests to each other but neither of them can
            // process the request because both of them are already holding the write lock.
            ctrl: &mut *self.ctrl_actor.try_write()?,
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
            last_index: self.command_log_actor.read().await.tail_pointer,
            snapshot_index: self.command_log_actor.read().await.snapshot_pointer,
            app_index: self.command_log_actor.read().await.app_pointer,
            commit_index: self.ctrl_actor.read().await.commit_pointer,
        };
        Ok(out)
    }

    pub(super) async fn get_membership(&self) -> Result<response::Membership> {
        let ballot = self.ctrl_actor.read().await.read_ballot().await?;

        let Some(leader_id) = ballot.voted_for else {
            bail!(Error::LeaderUnknown)
        };

        if self.ctrl_actor.read().await.is_leader() {
            let out = response::Membership {
                members: self.ctrl_actor.read().await.read_membership(),
            };
            Ok(out)
        } else {
            // Avoid looping.
            ensure!(self.io.local_server_id != leader_id);
            let conn = self.io.connect(&leader_id);
            let members = conn.get_membership().await?;
            Ok(response::Membership { members })
        }
    }

    pub async fn compare_term(&self, term: Term) -> Result<bool> {
        self.ctrl_actor.read().await.compare_term(term).await
    }

    pub async fn issue_read_index(&self) -> Result<Option<LogIndex>> {
        let ballot = self.ctrl_actor.read().await.read_ballot().await?;

        let Some(leader_id) = ballot.voted_for else {
            bail!(Error::LeaderUnknown)
        };

        if self.ctrl_actor.read().await.is_leader() {
            let read_index = self.ctrl_actor.read().await.find_read_index().await?;
            Ok(read_index)
        } else {
            // Avoid looping.
            ensure!(self.io.local_server_id != leader_id);
            let conn = self.io.connect(&leader_id);
            let resp = conn.issue_read_index().await?;
            Ok(resp)
        }
    }
}
