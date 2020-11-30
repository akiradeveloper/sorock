use crate::proto_compiled;
use crate::Id;
use std::time::Duration;
use tokio::sync::watch;

pub use tonic::transport::Endpoint;

pub type RaftClient = proto_compiled::raft_client::RaftClient<tonic::transport::Channel>;
pub async fn connect(endpoint: Endpoint) -> Result<RaftClient, tonic::Status> {
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

/// the purpose of gateway is to track the cluster members.
/// in Raft you can access the leader node if you know at least one node in the cluster
/// and gateway maintains the cluster members by polling the current membership.
pub mod gateway {
    use super::*;
    use crate::{core_message, thread_drop};
    use core::future::Future;
    use std::collections::HashSet;
    use std::sync::Arc;
    use tokio::sync::RwLock;

    #[derive(Clone)]
    pub struct CurrentMembership {
        pub list: Vec<Id>,
    }
    async fn query_new(list: Vec<Id>) -> anyhow::Result<(Option<Id>, Vec<Id>)> {
        exec(list, |id: Id| async move {
            let req = core_message::Req::ClusterInfo;
            let req = proto_compiled::ProcessReq {
                core: true,
                message: core_message::Req::serialize(&req),
            };
            let endpoint = Endpoint::from_shared(id)?;
            let mut conn = connect(endpoint).await?;
            let res = conn.request_process(req).await?.into_inner();
            let res = core_message::Rep::deserialize(&res.message).unwrap();
            if let core_message::Rep::ClusterInfo {
                leader_id,
                membership,
            } = res
            {
                Ok((leader_id, membership))
            } else {
                unreachable!()
            }
        })
        .await
    }
    fn sort(awared_leader: Id, awared_membership: Vec<Id>) -> Vec<Id> {
        let mut v = vec![];
        for member in awared_membership {
            let rank = if member == awared_leader { 0 } else { 1 };
            v.push((rank, member.to_owned()))
        }
        v.sort(); // leader first
        let mut r = vec![];
        for (_, id) in v {
            r.push(id)
        }
        r
    }
    pub fn watch(initial: HashSet<Id>) -> watch::Receiver<CurrentMembership> {
        let init_value = CurrentMembership {
            list: initial.into_iter().collect(),
        };
        let (tx, rx) = watch::channel(init_value);
        let rx_cln = rx.clone();
        tokio::spawn(async move {
            loop {
                let cur_list = rx_cln.borrow().clone().list;
                if let Ok((leader0, membership)) = query_new(cur_list).await {
                    // we don't trust the membership with no leader.
                    if let Some(leader) = leader0 {
                        let sorted = sort(leader, membership);
                        let _ = tx.broadcast(CurrentMembership { list: sorted });
                    }
                }
                tokio::time::delay_for(Duration::from_secs(5)).await;
            }
        });
        rx
    }
    pub async fn exec<D, F, T>(endpoints: impl IntoIterator<Item = D>, f: impl Fn(D) -> F) -> anyhow::Result<T>
    where
        F: Future<Output = anyhow::Result<T>>,
    {
        for endpoint in endpoints {
            if let Ok(res) = f(endpoint).await {
                return Ok(res);
            }
        }
        Err(anyhow::anyhow!(
            "any attempts to given endpoints ended up in failure"
        ))
    }
    pub async fn parallel<D, F, T>(endpoints: impl IntoIterator<Item = D>, f: impl Fn(D) -> F) -> Vec<anyhow::Result<T>>
    where
        F: Future<Output = anyhow::Result<T>>,
    {
        let mut futs = vec![];
        for endpoint in endpoints {
            futs.push(f(endpoint));
        }
        futures::future::join_all(futs).await
    }
}
