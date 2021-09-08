use crate::ui;
use futures::stream::Stream;
use futures::StreamExt;
use std::collections::{HashMap, HashSet};

#[derive(Clone, Debug, Default)]
pub struct ClusterStatus {
    pub leader_id: Option<String>,
    pub data: HashMap<String, NodeStatus>,
}

#[derive(Clone, Debug, Default)]
pub struct NodeStatus {
    pub log_info: Option<LogInfo>,
    pub health_ok: bool,
}

#[derive(Clone, Copy, Debug, Default)]
pub struct LogInfo {
    pub snapshot_index: u64,
    pub last_applied: u64,
    pub commit_index: u64,
    pub last_log_index: u64,
}

pub struct App<S> {
    pub running: bool,
    data_stream: S,
}
impl<S> App<S>
where
    S: Stream<Item = ClusterStatus> + Unpin,
{
    pub async fn new(s: S) -> Self {
        Self {
            running: true,
            data_stream: s,
        }
    }
    pub fn on_key(&mut self, c: char) {
        match c {
            'q' => self.running = false,
            _ => {}
        }
    }
    pub async fn make_model(&mut self) -> ui::Model {
        let cur_status = self.data_stream.next().await.unwrap_or(ClusterStatus {
            leader_id: None,
            data: HashMap::new(),
        });

        let mut members = vec![];
        for (id, node_status) in &cur_status.data {
            let id = id.to_owned();
            let alive = node_status.health_ok;
            let loginfo = node_status.log_info.unwrap_or(LogInfo::default());
            members.push(ui::Member {
                id: id,
                alive,
                snapshot_index: loginfo.snapshot_index,
                last_applied: loginfo.last_applied,
                commit_index: loginfo.commit_index,
            })
        }
        // the leader is first and slow nodes succeed.
        members.sort_by_key(|x| {
            if let Some(leader_id) = &cur_status.leader_id {
                if &x.id == leader_id {
                    -1 as i64
                } else {
                    x.commit_index as i64
                }
            } else {
                x.commit_index as i64
            }
        });

        ui::Model { members }
    }
}
