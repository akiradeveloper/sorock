use super::*;

pub mod request {
    use super::*;
    pub struct UserRequest {
        pub message: Bytes,
        pub mutation: bool,
    }
    pub struct KernRequest {
        pub message: Bytes,
    }
    pub struct Heartbeat {
        pub leader_id: NodeId,
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
}

pub mod response {
    use super::*;
    pub struct SendLogStream {
        pub success: bool,
        pub log_last_index: Index,
    }
    pub struct ClusterInfo {
        pub known_leader: Option<NodeId>,
        pub known_members: HashSet<NodeId>,
    }
}
