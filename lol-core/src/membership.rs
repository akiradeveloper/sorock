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
    async fn add_server<A: RaftApp>(
        &mut self,
        id: Id,
        core: Arc<RaftCore<A>>,
    ) -> anyhow::Result<()> {
        if self.membership.contains(&id) {
            return Ok(());
        }
        self.membership.insert(id.clone());
        if id != self.selfid {
            let mut dropper = ThreadDrop::new();
            let replication_thread = tokio::spawn(crate::thread::replication::run(
                Arc::clone(&core),
                id.clone(),
            ));
            dropper.register_abort_on_drop(replication_thread);
            let heartbeat_thread =
                tokio::spawn(crate::thread::heartbeat::run(Arc::clone(&core), id.clone()));
            dropper.register_abort_on_drop(heartbeat_thread);
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
    pub async fn set_membership<A: RaftApp>(
        &mut self,
        goal: &HashSet<Id>,
        core: Arc<RaftCore<A>>,
    ) -> anyhow::Result<()> {
        let cur = &self.membership;
        let (to_add, to_remove) = diff_set(cur, goal);
        // $4.4
        // When making cluster membership changes that require multiple single-server steps,
        // it is preferable to add servers before removing servers.
        for id in to_add {
            self.add_server(id, Arc::clone(&core)).await?;
        }
        for id in to_remove {
            self.remove_server(id);
        }
        Ok(())
    }
}
fn diff_set<T: Clone + Eq + std::hash::Hash>(
    cur: &HashSet<T>,
    goal: &HashSet<T>,
) -> (HashSet<T>, HashSet<T>) {
    let mut intersection = HashSet::new();
    for id in cur.intersection(goal) {
        intersection.insert(id.clone());
    }
    let to_add = goal - &intersection;
    let to_remove = cur - &intersection;
    (to_add, to_remove)
}
#[test]
fn test_diff_set() {
    use std::iter::FromIterator;
    let cur = HashSet::from_iter(vec![1, 2, 3, 4]);
    let goal = HashSet::from_iter(vec![3, 4, 5, 6]);
    let (to_add, to_remove) = diff_set(&cur, &goal);
    assert_eq!(to_add, HashSet::from_iter(vec![5, 6]));
    assert_eq!(to_remove, HashSet::from_iter(vec![1, 2]));
}
