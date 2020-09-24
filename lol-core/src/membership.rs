use super::thread_drop::ThreadDrop;
use crate::{connection, Id, Index};
use crate::{RaftApp, RaftCore};
use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::atomic::Ordering;
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
pub struct ClusterMember {
    pub endpoint: connection::Endpoint,
    pub progress: Option<ReplicationProgress>,
}
#[derive(Debug)]
pub struct Cluster {
    selfid: Id,
    pub internal: HashMap<Id, ClusterMember>,
    thread_drop: HashMap<Id, ThreadDrop>,
}
impl Cluster {
    pub async fn empty(id: Id) -> Self {
        Self {
            selfid: id,
            internal: HashMap::new(),
            thread_drop: HashMap::new(),
        }
    }
    pub fn id_list(&self) -> HashSet<Id> {
        self.internal.keys().cloned().collect()
    }
    pub async fn add_server<A: RaftApp>(&mut self, id: Id, core: Arc<RaftCore<A>>) {
        if self.internal.contains_key(&id) {
            return;
        }
        let endpoint = connection::Endpoint::new(id.clone());
        let member = if id == self.selfid {
            ClusterMember {
                endpoint,
                progress: None,
            }
        } else {
            let mut dropper = ThreadDrop::new();
            let g = dropper.register(crate::thread::replication::run(
                Arc::clone(&core),
                id.clone(),
            ));
            tokio::spawn(g);
            self.thread_drop.insert(id.clone(), dropper);

            let last_log_index = core.log.get_last_log_index().await;
            ClusterMember {
                endpoint,
                progress: Some(ReplicationProgress::new(last_log_index)),
            }
        };
        self.internal.insert(id, member);
    }
    pub fn remove_server(&mut self, id: Id) {
        if !self.internal.contains_key(&id) {
            return;
        }
        self.internal.remove(&id);
        self.thread_drop.remove(&id);
    }
    pub async fn set_membership<A: RaftApp>(&mut self, goal: &HashSet<Id>, core: Arc<RaftCore<A>>) {
        let cur = self.id_list();
        let mut to_add = HashSet::new();
        for x in goal {
            if !cur.contains(x) {
                to_add.insert(x.clone());
            }
        }
        let mut to_remove = HashSet::new();
        for x in &cur {
            if !goal.contains(x) {
                to_remove.insert(x.clone());
            }
        }
        for id in to_add {
            self.add_server(id, Arc::clone(&core)).await;
        }
        for id in to_remove {
            self.remove_server(id);
        }
    }
}
