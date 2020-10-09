use crate::protoimpl;
use crate::Id;
use std::time::Duration;

pub type Connection = protoimpl::raft_client::RaftClient<tonic::transport::Channel>;

#[derive(Clone)]
pub struct EndpointConfig {
    timeout: Option<Duration>,
}
impl EndpointConfig {
    pub fn default() -> Self {
        Self { timeout: None }
    }
    pub fn timeout(self, x: Duration) -> Self {
        Self {
            timeout: Some(x),
            ..self
        }
    }
}

#[derive(Clone, Debug)]
pub struct Endpoint {
    pub id: Id,
}
impl Endpoint {
    pub fn new(id: Id) -> Self {
        Self { id }
    }
    pub async fn connect(&self) -> Result<Connection, tonic::Status> {
        self.connect_with(EndpointConfig::default()).await
    }
    pub async fn connect_with(&self, config: EndpointConfig) -> Result<Connection, tonic::Status> {
        let id = format!("http://{}", self.id);
        let mut endpoint = tonic::transport::Endpoint::from_shared(id).unwrap();
        if let Some(x) = config.timeout {
            endpoint = endpoint.timeout(x);
        }
        protoimpl::raft_client::RaftClient::connect(endpoint)
            .await
            .map_err(|_| {
                tonic::Status::new(
                    tonic::Code::Unavailable,
                    format!("failed to connect to {}", self.id),
                )
            })
    }
}
/// resolve the socket address
pub fn resolve(id: &str) -> Option<String> {
    use std::net::ToSocketAddrs;
    let res = id.to_socket_addrs();
    match res {
        Ok(mut it) => {
            if let Some(x) = it.next() {
                Some(x.to_string())
            } else {
                None
            }
        }
        Err(_) => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_resolve() {
        assert!(resolve("localhost:80").is_some());
        assert!(resolve("locallocalhost:80").is_none());
        assert!(resolve("localhost:50051").is_some());
        assert!(resolve("127.0.0.1:80").is_some());
        assert!(resolve("127.0.0.1:50051").is_some());
        assert!(resolve("192.168.39.15:50051").is_some());
    }
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

    pub struct Gateway {
        awared_membership: HashSet<Id>,
        awared_leader: Option<Id>,
        thread_drop: thread_drop::ThreadDrop,
        cached: Vec<Endpoint>,
    }
    impl Gateway {
        /// build a gateway from a node list.
        /// the node list should include at least one node actually being in the cluster.
        pub async fn new(initial: HashSet<Id>) -> Arc<RwLock<Self>> {
            let gateway = Arc::new(RwLock::new(Self::new_inner(initial)));
            gateway.write().await.compute_cache();
            gateway
        }
        /// spawn companion thread the periodically refresh the
        /// internal cache of Gateway.
        pub async fn start_companion_thread(gateway: &Arc<RwLock<Self>>) {
            let cln = Arc::clone(&gateway);
            let companion = async move {
                loop {
                    tokio::time::delay_for(Duration::from_secs(5)).await;
                    let r = cln.read().await.query_new().await;
                    if let Ok((leader_id, membership)) = r {
                        cln.write().await.update_with(leader_id, membership);
                        cln.write().await.compute_cache();
                    }
                }
            };
            let abortable = gateway.write().await.thread_drop.register(companion);
            tokio::spawn(abortable);
        }
        fn new_inner(initial: HashSet<Id>) -> Self {
            Self {
                awared_membership: initial,
                awared_leader: None,
                thread_drop: thread_drop::ThreadDrop::new(),
                cached: vec![],
            }
        }
        fn compute_cache(&mut self) {
            let mut v = vec![];
            for member in &self.awared_membership {
                let rank = if Some(member) == self.awared_leader.as_ref() {
                    0
                } else {
                    1
                };
                v.push((rank, member.to_owned()))
            }
            v.sort(); // leader first
            let mut endpoints = vec![];
            for (_, member) in v {
                let endpoint = Endpoint::new(member);
                endpoints.push(endpoint);
            }
            self.cached = endpoints;
        }
        /// get the current node list tracked by this gateway.
        /// if there is a leader in the list the node comes first.
        pub fn query_sequence(&self) -> Vec<Endpoint> {
            self.cached.clone()
        }
        async fn query_new(&self) -> anyhow::Result<(Option<String>, Vec<String>)> {
            let endpoints = self.query_sequence();
            let config = EndpointConfig::default();
            exec(&config, &endpoints, |mut conn: Connection| async move {
                let req = core_message::Req::ClusterInfo;
                let req = protoimpl::ApplyReq {
                    mutation: false,
                    core: true,
                    message: core_message::Req::serialize(&req),
                };
                let res = conn.request_apply_immediate(req).await?.into_inner();
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
        fn update_with(&mut self, leader_id: Option<String>, membership: Vec<String>) {
            // if leader is not known, we don't use the value because
            // it can be during an election.
            if leader_id.is_none() {
                return;
            }
            self.awared_leader = leader_id;
            self.awared_membership = membership.into_iter().collect();
        }
    }
    pub async fn exec<F, T>(
        config: &EndpointConfig,
        endpoints: &[Endpoint],
        f: impl Fn(Connection) -> F,
    ) -> anyhow::Result<T>
    where
        F: Future<Output = anyhow::Result<T>>,
    {
        for endpoint in endpoints {
            if let Ok(conn) = endpoint.connect_with(config.clone()).await {
                if let Ok(res) = f(conn).await {
                    return Ok(res);
                }
            }
        }
        Err(anyhow::anyhow!(
            "any attempts to given endpoints ended up in failure"
        ))
    }
    pub async fn parallel<F, T>(
        config: &EndpointConfig,
        endpoints: &[Endpoint],
        f: impl Fn(Connection) -> F + Clone,
    ) -> Vec<anyhow::Result<T>>
    where
        F: Future<Output = anyhow::Result<T>>,
    {
        let mut futs = vec![];
        for endpoint in endpoints {
            let g = f.clone();
            futs.push(async move {
                let conn = endpoint.connect_with(config.clone()).await?;
                g(conn).await
            })
        }
        futures::future::join_all(futs).await
    }
}
