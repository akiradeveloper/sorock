use crate::connection::{Endpoint, EndpointConfig};
use crate::{ack, core_message, proto_compiled, Command, ElectionState, Clock, RaftApp, RaftCore};
use std::sync::Arc;
use std::time::Duration;
use std::net::SocketAddr;
use tokio::stream::StreamExt;

use proto_compiled::{
    raft_server::Raft,
    AppendEntryRep, AppendEntryReq, GetSnapshotReq,
    ApplyRep, ApplyReq, CommitRep, CommitReq, ProcessReq, ProcessRep,
    HeartbeatRep, HeartbeatReq, RequestVoteRep, RequestVoteReq, TimeoutNowRep, TimeoutNowReq,
    AddServerReq, AddServerRep, RemoveServerReq, RemoveServerRep,
};
// This code is expecting stream in a form
// Header (Entry Frame+)
async fn into_in_stream(mut out_stream: tonic::Streaming<AppendEntryReq>) -> crate::LogStream {
    use proto_compiled::append_entry_req::Elem;
    // header
    let (sender_id, prev_log_term, prev_log_index) = if let Some(Ok(chunk)) = out_stream.next().await {
        let e = chunk.elem.unwrap();
        if let Elem::Header(proto_compiled::AppendStreamHeader {
            sender_id,
            prev_log_index,
            prev_log_term,
        }) = e
        {
            (sender_id, prev_log_term, prev_log_index)
        } else {
            unreachable!()
        }
    } else {
        unreachable!()
    };
    let entries = async_stream::stream! {
        while let Some(Ok(chunk)) = out_stream.next().await {
            let e = chunk.elem.unwrap();
            match e {
                Elem::Entry(proto_compiled::AppendStreamEntry { term, index, command }) => {
                    let e = crate::LogStreamElem {
                        term,
                        index,
                        command: command.into(),
                    };
                    yield e;
                },
                _ => unreachable!(),
            }
        }
    };
    crate::LogStream {
        sender_id,
        prev_log_term,
        prev_log_index,
        entries: Box::pin(entries),
    }
}
pub struct Server<A: RaftApp> {
    pub core: Arc<RaftCore<A>>,
}
#[tonic::async_trait]
impl<A: RaftApp> Raft for Server<A> {
    async fn request_apply(
        &self,
        request: tonic::Request<ApplyReq>,
    ) -> Result<tonic::Response<ApplyRep>, tonic::Status> {
        let vote = self.core.load_vote().await.unwrap();
        if vote.voted_for.is_none() {
            return Err(tonic::Status::failed_precondition(
                "leader is not known by the server",
            ));
        }
        let leader_id = vote.voted_for.unwrap();

        if std::matches!(*self.core.election_state.read().await, ElectionState::Leader) {
            let (ack, rx) = ack::channel_for_apply();
            let req = request.into_inner();
            if req.mutation {
                let command = Command::Req {
                    core: req.core,
                    message: req.message.into(),
                };
                self.core.queue_entry(command, Some(ack)).await.unwrap();
            } else {
                self.core.register_query(req.core, req.message.into(), ack).await;
            }
            let res = rx.await;
            res.map(|x| tonic::Response::new(proto_compiled::ApplyRep { message: x.0 }))
                .map_err(|_| tonic::Status::cancelled("failed to apply the request"))
        } else {
            let endpoint = Endpoint::new(leader_id);
            let mut conn = endpoint.connect().await?;
            conn.request_apply(request).await
        }
    }
    async fn request_commit(
        &self,
        request: tonic::Request<CommitReq>,
    ) -> Result<tonic::Response<CommitRep>, tonic::Status> {
        let vote = self.core.load_vote().await.unwrap();
        if vote.voted_for.is_none() {
            return Err(tonic::Status::failed_precondition(
                "leader is not known by the server",
            ));
        }
        let leader_id = vote.voted_for.unwrap();

        if std::matches!(*self.core.election_state.read().await, ElectionState::Leader) {
            let (ack, rx) = ack::channel_for_commit();
            let req = request.into_inner();
            let command = if req.core {
                match core_message::Req::deserialize(&req.message).unwrap() {
                    core_message::Req::AddServer(id) => {
                        // I know this isn't correct in a real sense.
                        // As there is a gap between this guard and setting new barrier
                        // concurrent requests "can be" accepted but this is ok in practice.
                        if !self.core.allow_new_membership_change() {
                            return Err(tonic::Status::failed_precondition("concurrent membership change is not allowed."));
                        }

                        let mut membership = self.core.cluster.read().await.get_membership();
                        membership.insert(id);
                        Command::ClusterConfiguration { membership }
                    },
                    core_message::Req::RemoveServer(id) => {
                        if !self.core.allow_new_membership_change() {
                            return Err(tonic::Status::failed_precondition("concurrent membership change is not allowed."));
                        }

                        let mut membership = self.core.cluster.read().await.get_membership();
                        membership.remove(&id);
                        Command::ClusterConfiguration { membership }
                    },
                    _ => Command::Req {
                        message: req.message.into(),
                        core: req.core,
                    },
                }
            } else {
                Command::Req {
                    message: req.message.into(),
                    core: req.core,
                }
            };
            self.core.queue_entry(command, Some(ack)).await.unwrap();
            let res = rx.await;
            res.map(|_| tonic::Response::new(proto_compiled::CommitRep {}))
                .map_err(|_| tonic::Status::cancelled("failed to commit the request"))
        } else {
            let endpoint = Endpoint::new(leader_id);
            let mut conn = endpoint.connect().await?;
            conn.request_commit(request).await
        }
    }
    async fn request_process(
        &self,
        request: tonic::Request<ProcessReq>,
    ) -> Result<tonic::Response<ProcessRep>, tonic::Status> {
        let vote = self.core.load_vote().await.unwrap();
        if vote.voted_for.is_none() {
            return Err(tonic::Status::failed_precondition(
                "leader is not known by the server",
            ));
        }
        let leader_id = vote.voted_for.unwrap();

        if std::matches!(*self.core.election_state.read().await, ElectionState::Leader) {
            let req = request.into_inner();
            let res = if req.core {
                self.core.process_message(&req.message).await
            } else {
                self.core.app.process_message(&req.message).await
            };
            res.map(|x| tonic::Response::new(ProcessRep { message: x }))
                .map_err(|_| tonic::Status::unknown("failed to immediately apply the request"))
        } else {
            let endpoint = Endpoint::new(leader_id);
            let mut conn = endpoint.connect().await?;
            conn.request_process(request).await
        }
    }
    async fn request_process_locally(
        &self,
        request: tonic::Request<ProcessReq>,
    ) -> Result<tonic::Response<ProcessRep>, tonic::Status> {
        let req = request.into_inner();
        let res = if req.core {
            self.core.process_message(&req.message).await
        } else {
            self.core.app.process_message(&req.message).await
        };
        res.map(|x| tonic::Response::new(ProcessRep { message: x }))
            .map_err(|_| tonic::Status::unknown("failed to locally apply the request"))
    }
    async fn request_vote(
        &self,
        request: tonic::Request<RequestVoteReq>,
    ) -> Result<tonic::Response<RequestVoteRep>, tonic::Status> {
        let req = request.into_inner();
        let candidate_term = req.term;
        let candidate_id = req.candidate_id;
        let candidate_clock = Clock { term: req.last_log_term, index: req.last_log_index };
        let force_vote = req.force_vote;
        let vote_granted = self
            .core
            .receive_vote(candidate_term, candidate_id, candidate_clock, force_vote)
            .await.unwrap();
        let res = RequestVoteRep { vote_granted };
        Ok(tonic::Response::new(res))
    }
    async fn send_append_entry(
        &self,
        request: tonic::Request<tonic::Streaming<AppendEntryReq>>,
    ) -> Result<tonic::Response<AppendEntryRep>, tonic::Status> {
        let out_stream = request.into_inner();
        let in_stream = into_in_stream(out_stream).await;
        let success = self.core.queue_received_entry(in_stream).await.unwrap();
        let res = AppendEntryRep {
            success,
            last_log_index: self.core.log.get_last_log_index().await.unwrap(),
        };
        Ok(tonic::Response::new(res))
    }
    type GetSnapshotStream = crate::snapshot::SnapshotStreamOut;
    async fn get_snapshot(
        &self,
        request: tonic::Request<GetSnapshotReq>,
    ) -> Result<tonic::Response<Self::GetSnapshotStream>, tonic::Status> {
        let req = request.into_inner();
        let snapshot_index = req.index;
        let in_stream = self.core.make_snapshot_stream(snapshot_index).await.unwrap();
        if in_stream.is_none() {
            return Err(tonic::Status::not_found("requested snapshot is not in the inventory"));
        }
        let in_stream = in_stream.unwrap();
        Ok(tonic::Response::new(crate::snapshot::into_out_stream(in_stream)))
    }
    async fn send_heartbeat(
        &self,
        request: tonic::Request<HeartbeatReq>,
    ) -> Result<tonic::Response<HeartbeatRep>, tonic::Status> {
        let req = request.into_inner();
        let leader_id = req.leader_id;
        let term = req.term;
        let leader_commit = req.leader_commit;
        self.core
            .receive_heartbeat(leader_id, term, leader_commit)
            .await.unwrap();
        let res = HeartbeatRep {};
        Ok(tonic::Response::new(res))
    }
    async fn timeout_now(
        &self,
        _: tonic::Request<TimeoutNowReq>,
    ) -> Result<tonic::Response<TimeoutNowRep>, tonic::Status> {
        if std::matches!(*self.core.election_state.read().await, ElectionState::Follower) {
            self.core.try_promote(true).await.unwrap();
        }
        let res = TimeoutNowRep {};
        Ok(tonic::Response::new(res))
    }
    async fn add_server(
        &self,
        request: tonic::Request<AddServerReq>,
    ) -> Result<tonic::Response<AddServerRep>, tonic::Status> {
        let req = request.into_inner();
        let membership = self.core.cluster.read().await.get_membership();
        let ok = if membership.is_empty() && req.id == self.core.id {
            self.core.init_cluster().await.is_ok()
        } else {
            let msg = core_message::Req::AddServer(req.id);
            let req = proto_compiled::CommitReq {
                message: core_message::Req::serialize(&msg),
                core: true,
            };
            let endpoint = Endpoint::new(self.core.id.clone());
            let config = EndpointConfig::default().timeout(Duration::from_secs(5));
            let mut conn = endpoint.connect_with(config).await?;
            conn.request_commit(req).await.is_ok()
        };
        if ok {
            Ok(tonic::Response::new(AddServerRep {}))
        } else {
            Err(tonic::Status::aborted("couldn't add server"))
        }
    }
    async fn remove_server(
        &self,
        request: tonic::Request<RemoveServerReq>,
    ) -> Result<tonic::Response<RemoveServerRep>, tonic::Status> {
        let req = request.into_inner();
        let msg = core_message::Req::RemoveServer(req.id);
        let req = proto_compiled::CommitReq {
            message: core_message::Req::serialize(&msg),
            core: true,
        };
        let endpoint = Endpoint::new(self.core.id.clone());
        let config = EndpointConfig::default().timeout(Duration::from_secs(5));
        let mut conn = endpoint.connect_with(config).await?;
        let ok = conn.request_commit(req).await.is_ok();
        if ok {
            Ok(tonic::Response::new(RemoveServerRep {}))
        } else {
            Err(tonic::Status::aborted("couldn't remove server"))
        }
    }
}