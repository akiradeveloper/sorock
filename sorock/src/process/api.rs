use super::*;

pub mod request {
    use super::*;

    pub struct AppWriteRequest {
        pub message: Bytes,
        pub request_id: String,
    }

    pub struct AppReadRequest {
        pub message: Bytes,
    }

    pub struct KernelRequest {
        pub message: Bytes,
    }

    pub struct Heartbeat {
        pub sender_term: Term,
        pub sender_commit_index: LogIndex,
    }

    pub struct AddServer {
        pub server_id: ServerAddress,
    }

    pub struct RemoveServer {
        pub server_id: ServerAddress,
    }

    pub struct RequestVote {
        pub candidate_id: ServerAddress,
        pub candidate_clock: Clock,
        /// The term candidate try to promote at.
        pub vote_term: Term,
        /// $4.2.3
        /// If force_vote is set, the receiver server accepts the vote request
        /// regardless of the heartbeat timeout otherwise the vote request is
        /// dropped when it's receiving heartbeat.
        pub force_vote: bool,
        /// $9.6 Preventing disruptions when a server rejoins the cluster
        /// We recommend the Pre-Vote extension in deployments that would benefit from additional robustness.
        pub pre_vote: bool,
    }

    pub struct ReplicationStream {
        pub sender_id: ServerAddress,
        pub sender_term: Term,
        pub prev_clock: Clock,
        pub entries: std::pin::Pin<
            Box<dyn futures::stream::Stream<Item = Option<ReplicationStreamElem>> + Send>,
        >,
    }
    pub struct ReplicationStreamElem {
        pub this_clock: Clock,
        pub command: Bytes,
    }
}

pub mod response {
    use super::*;

    pub struct ReplicationStream {
        pub n_inserted: u64,
        pub log_last_index: LogIndex,
    }

    pub struct LogState {
        pub head_index: LogIndex,
        pub snapshot_index: LogIndex,
        pub app_index: LogIndex,
        pub commit_index: LogIndex,
        pub last_index: LogIndex,
    }

    pub struct Membership {
        pub members: HashSet<ServerAddress>,
    }
}
