use super::*;

mod cluster;
mod queue;
mod responder;

#[allow(dead_code)]
struct ThreadHandles {
    advance_kern_handle: thread::ThreadHandle,
    advance_user_handle: thread::ThreadHandle,
    advance_snapshot_handle: thread::ThreadHandle,
    advance_commit_handle: thread::ThreadHandle,
    election_handle: thread::ThreadHandle,
    log_compaction_handle: thread::ThreadHandle,
    query_execution_handle: thread::ThreadHandle,
    snapshot_deleter_handle: thread::ThreadHandle,
    stepdown_handle: thread::ThreadHandle,
}

/// `RaftProcess` is a implementation of Raft process in `RaftNode`.
/// `RaftProcess` is unaware of the gRPC and the network but just focuses on the Raft algorithm.
pub struct RaftProcess {
    command_log: CommandLog,
    voter: Voter,
    peers: PeerSvc,
    query_tx: query_queue::Producer,
    driver: RaftDriver,
    _thread_handles: ThreadHandles,

    queue_tx: thread::EventProducer<thread::QueueEvent>,
    replication_tx: thread::EventProducer<thread::ReplicationEvent>,
    app_tx: thread::EventProducer<thread::ApplicationEvent>,
}

impl RaftProcess {
    pub async fn new(
        app: impl RaftApp,
        log_store: impl RaftLogStore,
        ballot_store: impl RaftBallotStore,
        driver: RaftDriver,
    ) -> Result<Self> {
        let app = App::new(app);

        let (query_tx, query_rx) = query_queue::new(Ref(app.clone()));

        let command_log = CommandLog::new(log_store, app.clone());
        command_log.restore_state().await?;

        let (queue_tx, queue_rx) = thread::notify();
        let (replication_tx, replication_rx) = thread::notify();
        let (commit_tx, commit_rx) = thread::notify();
        let (kern_tx, kern_rx) = thread::notify();
        let (app_tx, app_rx) = thread::notify();

        let peers = PeerSvc::new(
            Ref(command_log.clone()),
            queue_rx.clone(),
            replication_tx.clone(),
            driver.clone(),
        );

        let voter = Voter::new(
            ballot_store,
            command_log.clone(),
            peers.clone(),
            driver.clone(),
        );

        peers.restore_state(Ref(voter.clone())).await?;

        let _thread_handles = ThreadHandles {
            advance_kern_handle: thread::advance_kern::new(
                command_log.clone(),
                voter.clone(),
                commit_rx.clone(),
                kern_tx.clone(),
            ),
            advance_user_handle: thread::advance_user::new(
                command_log.clone(),
                app.clone(),
                kern_rx.clone(),
                app_tx.clone(),
            ),
            advance_snapshot_handle: thread::advance_snapshot::new(command_log.clone()),
            advance_commit_handle: thread::advance_commit::new(
                command_log.clone(),
                Ref(peers.clone()),
                Ref(voter.clone()),
                replication_rx.clone(),
                commit_tx.clone(),
            ),
            election_handle: thread::election::new(voter.clone()),
            log_compaction_handle: thread::log_compaction::new(command_log.clone()),
            query_execution_handle: thread::query_execution::new(
                query_rx.clone(),
                Ref(command_log.clone()),
                app_rx.clone(),
            ),
            snapshot_deleter_handle: thread::snapshot_deleter::new(command_log.clone()),
            stepdown_handle: thread::stepdown::new(voter.clone()),
        };

        Ok(Self {
            command_log,
            voter,
            peers,
            query_tx,
            driver,
            _thread_handles,

            queue_tx,
            replication_tx,
            app_tx,
        })
    }
}
