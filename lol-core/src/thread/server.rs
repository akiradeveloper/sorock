use crate::connection::Endpoint;
use crate::{ack, core_message, protoimpl, Command, ElectionState, RaftApp, RaftCore};
use std::sync::atomic::Ordering;
use std::sync::Arc;
use tokio::stream::StreamExt;
use bytes::{Bytes, BytesMut};

use protoimpl::{
    raft_server::{Raft, RaftServer},
    AppendEntryRep, AppendEntryReq, GetSnapshotReq, GetSnapshotRep,
    ApplyRep, ApplyReq, CommitRep, CommitReq, ProcessReq, ProcessRep,
    HeartbeatRep, HeartbeatReq, RequestVoteRep, RequestVoteReq, TimeoutNowRep, TimeoutNowReq,
};

struct Thread<A: RaftApp> {
    core: Arc<RaftCore<A>>,
}
#[tonic::async_trait]
impl<A: RaftApp> Raft for Thread<A> {
    async fn request_apply(
        &self,
        request: tonic::Request<ApplyReq>,
    ) -> Result<tonic::Response<ApplyRep>, tonic::Status> {
        let vote = self.core.load_vote().await;
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
                    message: req.message,
                    core: req.core,
                };
                self.core.queue_entry(command, Some(ack)).await;
            } else {
                self.core.register_query(req.core, req.message.into(), ack).await;
            }
            let res = rx.await;
            res.map(|x| tonic::Response::new(protoimpl::ApplyRep { message: x.0 }))
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
        let vote = self.core.load_vote().await;
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
                        let mut membership = self.core.cluster.read().await.id_list();
                        membership.insert(id);
                        Command::ClusterConfiguration { membership }
                    },
                    core_message::Req::RemoveServer(id) => {
                        let mut membership = self.core.cluster.read().await.id_list();
                        membership.remove(&id);
                        Command::ClusterConfiguration { membership }
                    },
                    _ => Command::Req {
                        message: req.message,
                        core: req.core,
                    },
                }
            } else {
                Command::Req {
                    message: req.message,
                    core: req.core,
                }
            };
            self.core.queue_entry(command, Some(ack)).await;
            let res = rx.await;
            res.map(|_| tonic::Response::new(protoimpl::CommitRep {}))
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
        let vote = self.core.load_vote().await;
        if vote.voted_for.is_none() {
            return Err(tonic::Status::failed_precondition(
                "leader is not known by the server",
            ));
        }
        let leader_id = vote.voted_for.unwrap();

        if std::matches!(*self.core.election_state.read().await, ElectionState::Leader) {
            let req = request.into_inner();
            let res = if req.core {
                self.core.process_message(req.message.into()).await
            } else {
                self.core.app.process_message(req.message.into()).await
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
            self.core.process_message(req.message.into()).await
        } else {
            self.core.app.process_message(req.message.into()).await
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
        let candidate_clock = (req.last_log_term, req.last_log_index);
        let force_vote = req.force_vote;
        let vote_granted = self
            .core
            .receive_vote(candidate_term, candidate_id, candidate_clock, force_vote)
            .await;
        let res = RequestVoteRep { vote_granted };
        Ok(tonic::Response::new(res))
    }
    async fn send_append_entry(
        &self,
        request: tonic::Request<tonic::Streaming<AppendEntryReq>>,
    ) -> Result<tonic::Response<AppendEntryRep>, tonic::Status> {
        use protoimpl::append_entry_req::Elem;

        // This code is expecting stream in a form
        // Header (Entry Frame+)+

        let mut stream = request.into_inner();
        let mut req = if let Some(Ok(chunk)) = stream.next().await {
            let e = chunk.elem.unwrap();
            if let Elem::Header(protoimpl::AppendStreamHeader {
                sender_id,
                prev_log_index,
                prev_log_term,
            }) = e
            {
                crate::AppendEntryBuffer {
                    sender_id,
                    prev_log_index,
                    prev_log_term,
                    entries: vec![],
                }
            } else {
                unreachable!()
            }
        } else {
            unreachable!()
        };
        while let Some(Ok(chunk)) = stream.next().await {
            let e = chunk.elem.unwrap();
            match e {
                Elem::Entry(protoimpl::AppendStreamEntry { term, index, command }) => {
                    let e = crate::AppendEntryElem {
                        term,
                        index,
                        command: command.into(),
                    };
                    req.entries.push(e);
                },
                _ => unreachable!(),
            }
        }

        // TODO (optimization)
        // sadly, up to here, we put the entire received chunks on the heap.
        // we could make this lazy stream so the temporary allocation becomes less.
        let success = self.core.queue_received_entry(req).await;
        let res = AppendEntryRep {
            success,
            last_log_index: self.core.log.get_last_log_index().await,
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
        let st = self.core.make_snapshot_stream(snapshot_index).await;
        if st.is_none() {
            return Err(tonic::Status::not_found("requested snapshot is not in the inventory"));
        }
        let st = st.unwrap();
        Ok(tonic::Response::new(crate::snapshot::map_out(st)))
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
            .await;
        let res = HeartbeatRep {};
        Ok(tonic::Response::new(res))
    }
    async fn timeout_now(
        &self,
        request: tonic::Request<TimeoutNowReq>,
    ) -> Result<tonic::Response<TimeoutNowRep>, tonic::Status> {
        if std::matches!(*self.core.election_state.read().await, ElectionState::Follower) {
            self.core.try_promote(true).await;
        }
        let res = TimeoutNowRep {};
        Ok(tonic::Response::new(res))
    }
}
pub async fn run<A: RaftApp>(core: Arc<RaftCore<A>>) -> Result<(), tonic::transport::Error> {
    let addr = core.id.parse().unwrap();
    let th = Thread { core };
    tonic::transport::Server::builder()
        .add_service(RaftServer::new(th))
        .serve(addr)
        .await
}
