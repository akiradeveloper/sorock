use super::*;

use anyhow::Result;
use process::*;

mod stream;

pub struct Connection {
    cli: raft::RaftClient,
}
impl Connection {
    pub fn new(cli: raft::RaftClient) -> Self {
        Self { cli }
    }
}

impl Connection {
    pub async fn get_snapshot(&self, index: Index) -> Result<SnapshotStream> {
        let req = raft::GetSnapshotRequest { index };
        let st = self.cli.clone().get_snapshot(req).await?.into_inner();
        let st = Box::pin(stream::into_internal_snapshot_stream(st));
        Ok(st)
    }

    pub async fn send_heartbeat(&self, req: request::Heartbeat) -> Result<()> {
        let req = raft::Heartbeat {
            leader_id: req.leader_id.to_string(),
            leader_term: req.leader_term,
            leader_commit_index: req.leader_commit_index,
        };
        self.cli.clone().send_heartbeat(req).await?;
        Ok(())
    }

    pub async fn process_user_request(&self, req: request::UserRequest) -> Result<Bytes> {
        let req = raft::Request {
            message: req.message,
            mutation: req.mutation,
        };
        let resp = self.cli.clone().process(req).await?.into_inner();
        Ok(resp.message)
    }

    pub async fn process_kern_request(&self, req: request::KernRequest) -> Result<()> {
        let req = raft::KernRequest {
            message: req.message,
        };
        self.cli.clone().process_kern_request(req).await?;
        Ok(())
    }

    pub async fn send_timeout_now(&self) -> Result<()> {
        self.cli.clone().timeout_now(()).await?;
        Ok(())
    }

    pub async fn send_log_stream(&self, st: LogStream) -> Result<response::SendLogStream> {
        let st = stream::into_external_log_stream(st);
        let resp = self.cli.clone().send_log_stream(st).await?.into_inner();
        Ok(response::SendLogStream {
            success: resp.success,
            log_last_index: resp.log_last_index,
        })
    }

    pub async fn request_vote(&self, req: request::RequestVote) -> Result<bool> {
        let req = raft::VoteRequest {
            candidate_id: req.candidate_id.to_string(),
            candidate_clock: {
                let e = req.candidate_clock;
                Some(raft::Clock {
                    term: e.term,
                    index: e.index,
                })
            },
            vote_term: req.vote_term,
            force_vote: req.force_vote,
            pre_vote: req.pre_vote,
        };
        let resp = self.cli.clone().request_vote(req).await?.into_inner();
        Ok(resp.vote_granted)
    }
}
