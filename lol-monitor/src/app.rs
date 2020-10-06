use crate::ui;
use futures::stream::Stream;
use futures::StreamExt;
use std::collections::{HashMap, HashSet};

#[derive(Clone, Debug)]
pub struct Membership {
    pub leader_id: Option<String>,
    pub membership: Vec<String>,
}

#[derive(Clone, Debug)]
pub struct LogInfo {
    pub snapshot_index: u64,
    pub last_applied: u64,
    pub commit_index: u64,
    pub last_log_index: u64,
}

#[derive(Clone, Debug)]
pub struct HealthCheck {
    pub ok: bool,
}

pub struct App<S1, S2, S3> {
    pub running: bool,
    membership_channel: S1,
    loginfo_channel: S2,
    health_check_channel: S3,
}
impl<S1, S2, S3> App<S1, S2, S3>
where
    S1: Stream<Item = Membership> + Unpin,
    S2: Stream<Item = HashMap<String, LogInfo>> + Unpin,
    S3: Stream<Item = HashSet<String>> + Unpin,
{
    pub async fn new(s1: S1, s2: S2, s3: S3) -> Self {
        Self {
            running: true,
            membership_channel: s1,
            loginfo_channel: s2,
            health_check_channel: s3,
        }
    }
    pub fn on_key(&mut self, c: char) {
        match c {
            'q' => self.running = false,
            _ => {}
        }
    }
    pub async fn make_model(&mut self) -> ui::Model {
        let membership = self.membership_channel.next().await.unwrap_or(Membership {
            leader_id: None,
            membership: vec![],
        });
        let loginfos = self.loginfo_channel.next().await.unwrap_or(HashMap::new());
        let health_checks = self
            .health_check_channel
            .next()
            .await
            .unwrap_or(HashSet::new());

        let n = membership.membership.len();
        let mut members = vec![];
        for i in 0..n {
            let id = membership.membership[i].to_owned();
            let alive = health_checks.contains(&id);
            let loginfo = loginfos.get(&id).unwrap_or(&LogInfo {
                snapshot_index: 0,
                last_applied: 0,
                commit_index: 0,
                last_log_index: 0,
            });
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
            if let Some(leader_id) = &membership.leader_id {
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
