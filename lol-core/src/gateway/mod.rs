use std::collections::VecDeque;
use std::time::Duration;

use crate::proto_compiled::{raft_client::RaftClient, ClusterInfoReq};
use crate::{Id, Uri};
use tokio::sync::mpsc::error::TrySendError;
use tonic::transport::{Channel, Endpoint};
use tower::discover::Change;

mod service;

/// Gateway builder.
pub struct Connector {
    f: Box<dyn Fn(&Uri) -> Endpoint + 'static + Send>,
}
impl Connector {
    pub fn new(f: impl Fn(&Uri) -> Endpoint + 'static + Send) -> Self {
        Self { f: Box::new(f) }
    }
    pub fn connect(self, uri: Uri) -> Gateway {
        Gateway::new(uri.into(), self.f)
    }
}

/// Gateway is like `Channel` but updates the destination when cluster membership is changed.
/// With Gateway, users don't need to remember the current membership but can transparently
/// interact with the cluster through the current leader.
#[derive(Clone)]
pub struct Gateway {
    chan: Channel,
}
impl Gateway {
    fn new(id: Id, f: impl Fn(&Uri) -> Endpoint + 'static + Send) -> Self {
        let (chan, tx) = Channel::balance_channel::<Id>(16);
        tokio::spawn(async move {
            let mut cur_leader: Option<Id> = None;
            let mut new_leader: Option<Id> = None;
            let mut membership = vec![id];
            let mut change_queue = VecDeque::new();
            'outer: loop {
                for member in &membership {
                    let e = f(member.clone().uri());
                    if let Ok(mut conn) = RaftClient::connect(e).await {
                        let req = ClusterInfoReq {};
                        if let Ok(res) = conn.request_cluster_info(req).await {
                            let res = res.into_inner();
                            if let Some(leader) = res.leader_id {
                                let leader_id: Id = leader.parse().unwrap();
                                let mut xs = vec![];
                                for x in res.membership {
                                    xs.push(x.parse().unwrap());
                                }
                                new_leader = Some(leader_id.clone());
                                membership = Self::sort(leader_id, xs);
                                break;
                            }
                        }
                    }
                }
                if new_leader != cur_leader {
                    if let Some(ref new_leader) = new_leader {
                        let insert = Change::Insert(new_leader.clone(), f(new_leader.uri()));
                        change_queue.push_back(insert);
                        if let Some(ref cur_leader) = cur_leader {
                            let remove = Change::Remove(cur_leader.clone());
                            change_queue.push_back(remove);
                        }
                    }
                }
                loop {
                    if let Some(change) = change_queue.pop_front() {
                        let msg = match &change {
                            Change::Insert(k, e) => Change::Insert(k.clone(), e.clone()),
                            Change::Remove(k) => Change::Remove(k.clone()),
                        };
                        match tx.try_send(msg) {
                            Ok(()) => match change {
                                Change::Insert(k, _) => {
                                    cur_leader = Some(k.clone());
                                }
                                Change::Remove(_) => {}
                            },
                            Err(TrySendError::Full(_)) => {
                                change_queue.push_front(change);
                                break;
                            }
                            Err(TrySendError::Closed(_)) => {
                                break 'outer;
                            }
                        }
                    } else {
                        break;
                    }
                }
                tokio::time::sleep(Duration::from_secs(1)).await;
            } // outer loop
        });
        Self { chan }
    }
    fn sort(awared_leader: Id, awared_membership: Vec<Id>) -> Vec<Id> {
        let mut v = vec![];
        for member in awared_membership {
            let rank = if member == awared_leader { 0 } else { 1 };
            v.push((rank, member.to_owned()))
        }
        v.sort_by_key(|x| x.0); // leader first
        let mut r = vec![];
        for (_, id) in v {
            r.push(id)
        }
        r
    }
}
