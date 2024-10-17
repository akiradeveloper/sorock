use super::*;

pub mod request {
    use super::*;

    pub struct UserWriteRequest {
        pub message: Bytes,
        pub request_id: String,
    }

    pub struct UserReadRequest {
        pub message: Bytes,
    }

    pub struct KernRequest {
        pub message: Bytes,
    }

    pub struct Heartbeat {
        pub leader_term: Term,
        pub leader_commit_index: Index,
    }

    pub struct AddServer {
        pub server_id: NodeId,
    }

    pub struct RemoveServer {
        pub server_id: NodeId,
    }

    pub struct RequestVote {
        pub candidate_id: NodeId,
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
        pub sender_id: NodeId,
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
        pub log_last_index: Index,
    }
    pub struct LogState {
        pub head_index: Index,
        pub snap_index: Index,
        pub app_index: Index,
        pub commit_index: Index,
        pub last_index: Index,
    }
    pub struct Membership {
        pub members: HashSet<NodeId>,
    }
}
