use super::env::*;

use std::collections::HashSet;

use std::sync::Arc;
use std::time::Duration;
use lol_core::{connection, core_message, proto_compiled};
use tonic::transport::channel::Endpoint;
use tokio::runtime::Builder;

pub type Result<T> = anyhow::Result<T>;
pub type EnvRef = Arc<Environment>;

impl PartialEq for ClusterInfo {
    fn eq(&self, other: &Self) -> bool {
        self.leader_id == other.leader_id && self.membership == other.membership
    }
}
#[derive(Clone, Debug, Eq)]
pub struct ClusterInfo {
    pub leader_id: Option<lol_core::Id>,
    pub membership: Vec<lol_core::Id>,
}

pub struct Admin {
    to: String,
    env: EnvRef,
}
impl Admin {
    pub fn to(id: u8, env: EnvRef) -> Self {
        Self {
            to: env.get_node_id(id),
            env: Arc::clone(&env),
        }
    }
    fn block_on<R: Send + 'static>(fut: impl std::future::Future<Output = R> + Send + 'static) -> R {
        // since we can't nest a runtime in the same thread this workaround is needed.
        std::thread::spawn(|| {
            let rt = Builder::new_current_thread().enable_all().build().unwrap();
            rt.block_on(fut)
        }).join().unwrap()
    }
    pub fn add_server(&self, id: u8) -> Result<()> {
        let id = self.env.get_node_id(id);
        let req = proto_compiled::AddServerReq { id, };
        let endpoint = Endpoint::from_shared(self.to.clone())?.timeout(Duration::from_secs(5));
        Self::block_on(async move {
            let mut conn = connection::connect(endpoint).await?;
            conn.add_server(req).await
        })?;
        Ok(())
    }
    pub fn remove_server(&self, id: u8) -> Result<()> {
        let id = self.env.get_node_id(id);
        let req = proto_compiled::RemoveServerReq { id, };
        let endpoint = Endpoint::from_shared(self.to.clone())?.timeout(Duration::from_secs(5));
        Self::block_on(async move {
            let mut conn = connection::connect(endpoint).await?;
            conn.remove_server(req).await
        })?;
        Ok(())
    }
    pub fn cluster_info(&self) -> Result<ClusterInfo> {
        let msg = core_message::Req::ClusterInfo;
        let req = proto_compiled::ProcessReq {
            message: core_message::Req::serialize(&msg),
            core: true,
        };
        let endpoint = Endpoint::from_shared(self.to.clone())?.timeout(Duration::from_secs(5));
        let res = Self::block_on(async move {
            let mut conn = connection::connect(endpoint).await?;
            conn.request_process_locally(req).await
        })?.into_inner();
        let msg = core_message::Rep::deserialize(&res.message).unwrap();
        let msg = if let core_message::Rep::ClusterInfo {
            leader_id,
            membership,
        } = msg
        {
            ClusterInfo {
                leader_id,
                membership,
            }
        } else {
            unreachable!()
        };
        Ok(msg)
    }
    pub fn timeout_now(&self) -> Result<()> {
        let req = proto_compiled::TimeoutNowReq {};
        let endpoint = Endpoint::from_shared(self.to.clone())?.timeout(Duration::from_secs(5));
        Self::block_on(async move {
            let mut conn = connection::connect(endpoint).await?;
            conn.timeout_now(req).await
        })?;
        Ok(())
    }
}
/// Wait for every node in `live_nodes` recognize the same membership.
pub fn ensure_membership(timeout: Duration, live_nodes: Vec<u8>, member_nodes: Vec<u8>, env: EnvRef) {
    let mut leader_can_be = HashSet::new();
    for &id in &live_nodes {
        leader_can_be.insert(Some(env.get_node_id(id)));
    }
    let mut membership_should_be = vec![];
    for id in member_nodes {
        membership_should_be.push(env.get_node_id(id));
    }
    membership_should_be.sort();
    // wait for the agreed value being returned
    let res = wait_for_consensus(timeout, live_nodes, |id| {
        Admin::to(id, env.clone()).cluster_info().ok()
    });
    assert!(res.is_some());

    let res = res.unwrap();
    assert_eq!(res.membership, membership_should_be);
    assert!(leader_can_be.contains(&res.leader_id));
}
/// Make a cluster with a single node.
pub fn env_new(id: u8, command: NodeCommand) -> EnvRef {
    let env = Environment::new();
    env.start(id, command);
    // wait for the server is ready
    std::thread::sleep(Duration::from_millis(1000));
    // init-cluster send timeout-now internally to immediate vote for the new leader.
    Admin::to(id, env.clone()).add_server(id).unwrap();
    std::thread::sleep(Duration::from_millis(100));
    env
}
/// Make a cluster with the same command.
pub fn make_cluster(n: u8, command: impl Fn(u8) -> NodeCommand) -> EnvRef {
    let env = env_new(0, command(0).clone());
    for k in 1..n {
        env.start(k, command(k).clone());
        Admin::to(0, env.clone()).add_server(k).unwrap();
        let mut nodes = vec![];
        for i in 0..=k {
            nodes.push(i);
        }
        ensure_membership(Duration::from_secs(5), nodes.clone(), nodes, env.clone());
    }
    env
}