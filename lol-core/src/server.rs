use crate::{
    ack, core_message, proto_compiled, Clock, Command, ElectionState, Id, RaftApp, RaftCore,
};
use std::sync::Arc;
use std::time::Duration;
use tokio_stream::StreamExt;
use tonic::transport::Endpoint;

use proto_compiled::{
    raft_client::RaftClient, raft_server::Raft, AddServerRep, AddServerReq, AppendEntryRep,
    AppendEntryReq, ApplyRep, ApplyReq, ClusterInfoRep, ClusterInfoReq, CommitRep, CommitReq,
    GetConfigRep, GetConfigReq, GetSnapshotReq, HeartbeatRep, HeartbeatReq,
    RemoveServerRep, RemoveServerReq, RequestVoteRep, RequestVoteReq, StatusRep, StatusReq,
    TimeoutNowRep, TimeoutNowReq, TuneConfigRep, TuneConfigReq,
};
async fn connect(
    endpoint: Endpoint,
) -> Result<RaftClient<tonic::transport::Channel>, tonic::Status> {
    let uri = endpoint.uri().clone();
    proto_compiled::raft_client::RaftClient::connect(endpoint)
        .await
        .map_err(|_| {
            tonic::Status::new(
                tonic::Code::Unavailable,
                format!("failed to connect to {}", uri),
            )
        })
}
// This code is expecting stream in a form
// Header (Entry Frame+)
async fn into_in_stream(mut out_stream: tonic::Streaming<AppendEntryReq>) -> crate::LogStream {
    use proto_compiled::append_entry_req::Elem;
    // Header
    let (sender_id, prev_log_term, prev_log_index) =
        if let Some(Ok(chunk)) = out_stream.next().await {
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
                        command: command,
                    };
                    yield e;
                },
                _ => unreachable!(),
            }
        }
    };
    crate::LogStream {
        sender_id: sender_id.parse().unwrap(),
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
    async fn get_config(
        &self,
        _: tonic::Request<GetConfigReq>,
    ) -> Result<tonic::Response<GetConfigRep>, tonic::Status> {
        let core = &self.core;
        match core.tunable.try_read() {
            Ok(tunable) => {
                let rep = GetConfigRep {
                    compaction_delay_sec: tunable.compaction_delay_sec,
                    compaction_interval_sec: tunable.compaction_interval_sec,
                };
                Ok(tonic::Response::new(rep))
            }
            Err(poisoned_error) => Err(tonic::Status::internal(format!(
                "cannot update tunable configuration: state is poisoned({})",
                poisoned_error
            ))),
        }
    }
    async fn status(
        &self,
        _: tonic::Request<StatusReq>,
    ) -> Result<tonic::Response<StatusRep>, tonic::Status> {
        use std::sync::atomic::Ordering;
        let core = &self.core;
        let rep = StatusRep {
            snapshot_index: core.log.get_snapshot_index(),
            last_applied: core.log.last_applied.load(Ordering::SeqCst),
            commit_index: core.log.commit_index.load(Ordering::SeqCst),
            last_log_index: core.log.get_last_log_index().await.unwrap(),
        };
        Ok(tonic::Response::new(rep))
    }
    async fn request_cluster_info(
        &self,
        _: tonic::Request<ClusterInfoReq>,
    ) -> Result<tonic::Response<ClusterInfoRep>, tonic::Status> {
        let leader_id = match self.core.load_ballot().await {
            Ok(x) => x.voted_for.map(|x| x.to_string()),
            Err(_) => return Err(tonic::Status::internal("could not get ballot")),
        };
        let rep = ClusterInfoRep {
            leader_id,
            membership: {
                let membership = self.core.cluster.read().await.membership.clone();
                let mut xs: Vec<_> = membership.into_iter().map(|x| x.to_string()).collect();
                xs.sort();
                xs
            },
        };
        Ok(tonic::Response::new(rep))
    }
    async fn tune_config(
        &self,
        request: tonic::Request<TuneConfigReq>,
    ) -> Result<tonic::Response<TuneConfigRep>, tonic::Status> {
        let req: TuneConfigReq = request.into_inner();
        match self.core.tunable.try_write() {
            Ok(mut tunable) => {
                req.compaction_delay_sec
                    .map(|value| (*tunable).compaction_delay_sec = value);
                req.compaction_interval_sec
                    .map(|value| (*tunable).compaction_interval_sec = value);
                Ok(tonic::Response::new(proto_compiled::TuneConfigRep {}))
            }
            Err(poisoned_error) => Err(tonic::Status::internal(format!(
                "cannot update tunable configuration: state is poisoned({})",
                poisoned_error
            ))),
        }
    }
    async fn request_apply(
        &self,
        request: tonic::Request<ApplyReq>,
    ) -> Result<tonic::Response<ApplyRep>, tonic::Status> {
        let ballot = self.core.load_ballot().await.unwrap();
        if ballot.voted_for.is_none() {
            return Err(tonic::Status::failed_precondition(
                "leader is not known by the server",
            ));
        }
        let leader_id = ballot.voted_for.unwrap();

        if std::matches!(
            *self.core.election_state.read().await,
            ElectionState::Leader
        ) {
            let (ack, rx) = ack::channel_for_apply();
            let req = request.into_inner();
            if req.mutation {
                let command = Command::Req {
                    core: req.core,
                    message: &req.message,
                };
                self.core
                    .queue_entry(Command::serialize(&command), Some(ack))
                    .await
                    .unwrap();
            } else {
                self.core
                    .register_query(req.core, req.message.into(), ack)
                    .await;
            }
            let res = rx.await;
            res.map(|x| tonic::Response::new(proto_compiled::ApplyRep { message: x.0 }))
                .map_err(|_| tonic::Status::cancelled("failed to apply the request"))
        } else {
            let endpoint = Endpoint::from(leader_id.uri().clone());
            let mut conn = connect(endpoint).await?;
            conn.request_apply(request).await
        }
    }
    async fn request_commit(
        &self,
        request: tonic::Request<CommitReq>,
    ) -> Result<tonic::Response<CommitRep>, tonic::Status> {
        let ballot = self.core.load_ballot().await.unwrap();
        if ballot.voted_for.is_none() {
            return Err(tonic::Status::failed_precondition(
                "leader is not known by the server",
            ));
        }
        let leader_id = ballot.voted_for.unwrap();

        if std::matches!(
            *self.core.election_state.read().await,
            ElectionState::Leader
        ) {
            let (ack, rx) = ack::channel_for_commit();
            let req = request.into_inner();
            let command = if req.core {
                match core_message::Req::deserialize(&req.message).unwrap() {
                    core_message::Req::AddServer(id) => {
                        // I know this isn't correct in a real sense.
                        // As there is a gap between this guard and setting new barrier
                        // concurrent requests "can be" accepted but this is ok in practice.
                        if !self.core.allow_new_membership_change() {
                            return Err(tonic::Status::failed_precondition(
                                "concurrent membership change is not allowed.",
                            ));
                        }

                        let mut membership = self.core.cluster.read().await.membership.clone();
                        membership.insert(id);
                        Command::ClusterConfiguration { membership }
                    }
                    core_message::Req::RemoveServer(id) => {
                        if !self.core.allow_new_membership_change() {
                            return Err(tonic::Status::failed_precondition(
                                "concurrent membership change is not allowed.",
                            ));
                        }

                        let mut membership = self.core.cluster.read().await.membership.clone();
                        membership.remove(&id);
                        Command::ClusterConfiguration { membership }
                    }
                    _ => Command::Req {
                        message: &req.message,
                        core: req.core,
                    },
                }
            } else {
                Command::Req {
                    message: &req.message,
                    core: req.core,
                }
            };
            self.core
                .queue_entry(Command::serialize(&command), Some(ack))
                .await
                .unwrap();
            let res = rx.await;
            res.map(|_| tonic::Response::new(proto_compiled::CommitRep {}))
                .map_err(|_| tonic::Status::cancelled("failed to commit the request"))
        } else {
            let endpoint = Endpoint::from(leader_id.uri().clone());
            let mut conn = connect(endpoint).await?;
            conn.request_commit(request).await
        }
    }
    async fn request_vote(
        &self,
        request: tonic::Request<RequestVoteReq>,
    ) -> Result<tonic::Response<RequestVoteRep>, tonic::Status> {
        let req = request.into_inner();
        let candidate_term = req.term;
        let candidate_id = req.candidate_id.parse().unwrap();
        let candidate_clock = Clock {
            term: req.last_log_term,
            index: req.last_log_index,
        };
        let force_vote = req.force_vote;
        let pre_vote = req.pre_vote;
        let vote_granted = self
            .core
            .receive_vote(
                candidate_term,
                candidate_id,
                candidate_clock,
                force_vote,
                pre_vote,
            )
            .await
            .unwrap();
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
        let in_stream = self
            .core
            .make_snapshot_stream(snapshot_index)
            .await
            .unwrap();
        if in_stream.is_none() {
            return Err(tonic::Status::not_found(
                "requested snapshot is not in the inventory",
            ));
        }
        let in_stream = in_stream.unwrap();
        Ok(tonic::Response::new(crate::snapshot::into_out_stream(
            in_stream,
        )))
    }
    async fn send_heartbeat(
        &self,
        request: tonic::Request<HeartbeatReq>,
    ) -> Result<tonic::Response<HeartbeatRep>, tonic::Status> {
        let req = request.into_inner();
        let leader_id = req.leader_id;
        let leader_id = leader_id.parse().unwrap();
        let term = req.term;
        let leader_commit = req.leader_commit;
        self.core
            .receive_heartbeat(leader_id, term, leader_commit)
            .await
            .unwrap();
        let res = HeartbeatRep {};
        Ok(tonic::Response::new(res))
    }
    async fn timeout_now(
        &self,
        _: tonic::Request<TimeoutNowReq>,
    ) -> Result<tonic::Response<TimeoutNowRep>, tonic::Status> {
        if std::matches!(
            *self.core.election_state.read().await,
            ElectionState::Follower
        ) {
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
        let add_server_id = req.id.parse().unwrap();
        let ok = if self.core.cluster.read().await.membership.is_empty()
            && add_server_id == self.core.id
        {
            self.core.init_cluster().await.is_ok()
        } else {
            let msg = core_message::Req::AddServer(add_server_id);
            let req = proto_compiled::CommitReq {
                message: core_message::Req::serialize(&msg),
                core: true,
            };
            let endpoint =
                Endpoint::from(self.core.id.uri().clone()).timeout(Duration::from_secs(5));
            let mut conn = connect(endpoint).await?;
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
        let remove_server_id = req.id.parse().unwrap();
        let msg = core_message::Req::RemoveServer(remove_server_id);
        let req = proto_compiled::CommitReq {
            message: core_message::Req::serialize(&msg),
            core: true,
        };
        let endpoint = Endpoint::from(self.core.id.uri().clone()).timeout(Duration::from_secs(5));
        let mut conn = connect(endpoint).await?;
        let ok = conn.request_commit(req).await.is_ok();
        if ok {
            Ok(tonic::Response::new(RemoveServerRep {}))
        } else {
            Err(tonic::Status::aborted("couldn't remove server"))
        }
    }
}
