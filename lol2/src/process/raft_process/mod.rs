use super::*;

mod cluster;
mod queue;
mod responder;

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

pub struct RaftProcess {
    command_log: CommandLog,
    voter: Voter,
    peers: PeerSvc,
    query_queue: QueryQueue,
    driver: RaftDriver,
    _thread_handles: ThreadHandles,
}

impl RaftProcess {
    pub async fn new(
        app: impl RaftApp,
        log_store: impl RaftLogStore,
        ballot_store: impl RaftBallotStore,
        driver: RaftDriver,
    ) -> Result<Self> {
        let app = App::new(app);

        let query_queue = QueryQueue::new(Ref(app.clone()));

        let command_log = CommandLog::new(log_store, app.clone());
        command_log.restore_state().await?;

        let peers = PeerSvc::new(Ref(command_log.clone()), driver.clone());

        let voter = Voter::new(
            ballot_store,
            command_log.clone(),
            peers.clone(),
            driver.clone(),
        );

        peers.restore_state(Ref(voter.clone())).await?;

        let _thread_handles = ThreadHandles {
            advance_kern_handle: thread::advance_kern::new(command_log.clone(), voter.clone()),
            advance_user_handle: thread::advance_user::new(command_log.clone(), app.clone()),
            advance_snapshot_handle: thread::advance_snapshot::new(command_log.clone()),
            advance_commit_handle: thread::advance_commit::new(
                command_log.clone(),
                Ref(peers.clone()),
                Ref(voter.clone()),
            ),
            election_handle: thread::election::new(voter.clone()),
            log_compaction_handle: thread::log_compaction::new(command_log.clone()),
            query_execution_handle: thread::query_execution::new(
                query_queue.clone(),
                Ref(command_log.clone()),
            ),
            snapshot_deleter_handle: thread::snapshot_deleter::new(command_log.clone()),
            stepdown_handle: thread::stepdown::new(voter.clone()),
        };

        Ok(Self {
            command_log,
            voter,
            peers,
            query_queue,
            driver,
            _thread_handles,
        })
    }
}
