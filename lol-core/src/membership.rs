use super::thread_drop::ThreadDrop;
use crate::{Id, Index};
use crate::{RaftApp, RaftCore};
use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;

#[derive(Clone, Copy, Debug)]
pub struct ReplicationProgress {
    pub next_index: Index,
    pub next_max_cnt: Index,
    pub match_index: Index,
}
impl ReplicationProgress {
    pub fn new(last_log_index: Index) -> Self {
        Self {
            next_index: last_log_index,
            next_max_cnt: 1,
            match_index: 0,
        }
    }
}
#[derive(Clone, Debug)]
pub struct Peer {
    pub progress: ReplicationProgress,
}
#[derive(Debug)]
pub struct Cluster {
    selfid: Id,
    pub membership: HashSet<Id>,
    pub peers: HashMap<Id, Peer>,
    thread_drop: HashMap<Id, ThreadDrop>,
}
impl Cluster {
    pub async fn empty(id: Id) -> Self {
        Self {
            selfid: id,
            membership: HashSet::new(),
            peers: HashMap::new(),
            thread_drop: HashMap::new(),
        }
    }
    async fn add_server<A: RaftApp>(&mut self, id: Id, core: Arc<RaftCore<A>>) -> anyhow::Result<()> {
        if self.membership.contains(&id) {
            return Ok(());
        }
        self.membership.insert(id.clone());
        if id != self.selfid {
            let mut dropper = ThreadDrop::new();
            let replication_thread = dropper.register(crate::thread::replication::run(
                Arc::clone(&core),
                id.clone(),
            ));
            tokio::spawn(replication_thread);
            let heartbeat_thread = dropper.register(crate::thread::heartbeat::run(
                Arc::clone(&core),
                id.clone(),
            ));
            tokio::spawn(heartbeat_thread);
            self.thread_drop.insert(id.clone(), dropper);

            let last_log_index = core.log.get_last_log_index().await?;
            let member = Peer {
                progress: ReplicationProgress::new(last_log_index),
            };
            self.peers.insert(id, member);
        };
        Ok(())
    }
    fn remove_server(&mut self, id: Id) {
        if !self.membership.contains(&id) {
            return;
        }
        self.membership.remove(&id);
        self.peers.remove(&id);
        self.thread_drop.remove(&id);
    }
    pub async fn set_membership<A: RaftApp>(&mut self, goal: &HashSet<Id>, core: Arc<RaftCore<A>>) -> anyhow::Result<()> {
        let cur = &self.membership;
        let mut to_add = HashSet::new();
        for x in goal {
            if !cur.contains(x) {
                to_add.insert(x.clone());
            }
        }
        let mut to_remove = HashSet::new();
        for x in cur {
            if !goal.contains(x) {
                to_remove.insert(x.clone());
            }
        }
        for id in to_add {
            self.add_server(id, Arc::clone(&core)).await?;
        }
        for id in to_remove {
            self.remove_server(id);
        }
        Ok(())
    }
}
