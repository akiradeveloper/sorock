use super::*;

use std::collections::HashMap;

pub struct HeartbeatBuffer {
    buf: crossbeam::queue::SegQueue<(ShardId, request::Heartbeat)>,
}
impl HeartbeatBuffer {
    pub fn new() -> Self {
        Self {
            buf: crossbeam::queue::SegQueue::new(),
        }
    }

    pub fn push(&self, shard_id: ShardId, req: request::Heartbeat) {
        self.buf.push((shard_id, req));
    }

    fn drain(&self) -> HashMap<ShardId, request::Heartbeat> {
        let mut out = HashMap::new();
        let n = self.buf.len();
        for _ in 0..n {
            let (k, v) = self.buf.pop().unwrap();
            out.insert(k, v);
        }
        out
    }
}

pub async fn run(buf: Arc<HeartbeatBuffer>, mut cli: raft::RaftClient, self_node_id: NodeId) {
    loop {
        tokio::time::sleep(Duration::from_millis(300)).await;

        let heartbeats = buf.drain();

        let states = {
            let mut out = HashMap::new();
            for (shard_id, heartbeat) in heartbeats {
                let state = raft::LeaderCommitState {
                    leader_term: heartbeat.leader_term,
                    leader_commit_index: heartbeat.leader_commit_index,
                };
                out.insert(shard_id, state);
            }
            out
        };

        let req = raft::Heartbeat {
            leader_id: self_node_id.to_string(),
            leader_commit_states: states,
        };
        cli.send_heartbeat(req).await.ok();
    }
}
