use super::*;

use std::collections::HashMap;
use std::sync::Mutex;

pub struct HeartbeatBuffer {
    buf: HashMap<ShardId, request::Heartbeat>,
}
impl HeartbeatBuffer {
    pub fn new() -> Self {
        Self {
            buf: HashMap::new(),
        }
    }

    pub fn push(&mut self, shard_id: ShardId, req: request::Heartbeat) {
        self.buf.insert(shard_id, req);
    }

    fn drain(&mut self) -> HashMap<ShardId, request::Heartbeat> {
        self.buf.drain().collect()
    }
}

pub async fn run(
    buf: Arc<Mutex<HeartbeatBuffer>>,
    mut cli: raft::RaftClient,
    self_node_id: NodeId,
) {
    loop {
        tokio::time::sleep(Duration::from_millis(300)).await;

        let heartbeats = {
            let mut buf = buf.lock().unwrap();
            let out = buf.drain();
            out
        };

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
