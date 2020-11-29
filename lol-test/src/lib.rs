use std::collections::{HashMap, HashSet};
use std::net::TcpListener;
use std::process::Child;
use std::sync::{Arc, RwLock};
use std::thread;
use std::time::Duration;
use lol_core::{connection, core_message, proto_compiled};

pub type Result<T> = anyhow::Result<T>;
pub type EnvRef = Arc<Environment>;

pub mod admin {
    #[derive(Clone, Debug)]
    pub struct InitCluster {
        pub ok: bool,
    }
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
}
use tokio::runtime::Builder;
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
            let mut rt = Builder::new().basic_scheduler().enable_all().build().unwrap();
            rt.block_on(fut)
        }).join().unwrap()
    }
    #[deprecated]
    fn init_cluster(&self) -> Result<admin::InitCluster> {
        let msg = core_message::Req::InitCluster;
        let req = proto_compiled::ProcessReq {
            message: core_message::Req::serialize(&msg),
            core: true,
        };
        let endpoint = connection::Endpoint::from_shared(self.to.clone())?.timeout(Duration::from_secs(5));
        let res = Self::block_on(async move {
            let mut conn = connection::connect(endpoint).await?;
            conn.request_process_locally(req).await
        })?.into_inner();
        let msg = core_message::Rep::deserialize(&res.message).unwrap();
        let msg = if let core_message::Rep::InitCluster { ok } = msg {
            admin::InitCluster { ok }
        } else {
            unreachable!()
        };
        Ok(msg)
    }
    pub fn add_server(&self, id: u8) -> Result<()> {
        let id = self.env.get_node_id(id);
        let req = proto_compiled::AddServerReq { id, };
        let endpoint = connection::Endpoint::from_shared(self.to.clone())?.timeout(Duration::from_secs(5));
        Self::block_on(async move {
            let mut conn = connection::connect(endpoint).await?;
            conn.add_server(req).await
        })?;
        Ok(())
    }
    pub fn remove_server(&self, id: u8) -> Result<()> {
        let id = self.env.get_node_id(id);
        let req = proto_compiled::RemoveServerReq { id, };
        let endpoint = connection::Endpoint::from_shared(self.to.clone())?.timeout(Duration::from_secs(5));
        Self::block_on(async move {
            let mut conn = connection::connect(endpoint).await?;
            conn.remove_server(req).await
        })?;
        Ok(())
    }
    pub fn cluster_info(&self) -> Result<admin::ClusterInfo> {
        let msg = core_message::Req::ClusterInfo;
        let req = proto_compiled::ProcessReq {
            message: core_message::Req::serialize(&msg),
            core: true,
        };
        let endpoint = connection::Endpoint::from_shared(self.to.clone())?.timeout(Duration::from_secs(5));
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
            admin::ClusterInfo {
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
        let endpoint = connection::Endpoint::from_shared(self.to.clone())?.timeout(Duration::from_secs(5));
        Self::block_on(async move {
            let mut conn = connection::connect(endpoint).await?;
            conn.timeout_now(req).await
        })?;
        Ok(())
    }
}
struct Node {
    child: Child,
}
impl Node {
    fn pid(&self) -> u32 {
        self.child.id()
    }
    fn pause(&mut self) {
        std::process::Command::new("kill")
            .arg("-STOP")
            .arg(&format!("{}", self.pid()))
            .spawn()
            .expect("failed to pause node");
    }
    fn unpause(&mut self) {
        std::process::Command::new("kill")
            .arg("-CONT")
            .arg(&format!("{}", self.pid()))
            .spawn()
            .expect("failed to unpause node");
    }
}
impl Drop for Node {
    fn drop(&mut self) {
        let _ = self.child.kill();
    }
}
#[derive(Clone)]
pub struct NodeCommand {
    name: String,
    pub args: Vec<String>,
}
impl NodeCommand {
    pub fn new(name: &str) -> Self {
        Self {
            name: name.to_owned(),
            args: vec![]
        }
    }
    pub fn with_args<'a, I: IntoIterator<Item=&'a str>>(self, args: I) -> Self {
        Self {
            name: self.name,
            args: args.into_iter().map(|x| x.to_owned()).collect()
        }
    }
}
pub struct Environment {
    port_list: RwLock<HashMap<u8, u16>>,
    node_list: RwLock<HashMap<u8, Node>>,
}
fn available_port() -> std::io::Result<u16> {
    TcpListener::bind("localhost:0").map(|x| x.local_addr().unwrap().port())
}
impl Environment {
    /// make a cluster of only one node
    pub fn new(id: u8, command: NodeCommand) -> Arc<Self> {
        thread::sleep(Duration::from_secs(1));
        let x = Self {
            port_list: RwLock::new(HashMap::new()),
            node_list: RwLock::new(HashMap::new()),
        };
        x.start(id, command);
        thread::sleep(Duration::from_millis(1000));
        let env = Arc::new(x);
        Admin::to(id, env.clone()).add_server(id).unwrap();
        thread::sleep(Duration::from_millis(2000));
        env
    }
    pub fn get_node_id(&self, id: u8) -> String {
        let port_list = self.port_list.read().unwrap();
        let port = port_list.get(&id).unwrap();
        format!("http://localhost:{}", port)
    }
    pub fn start(&self, id: u8, command: NodeCommand) {
        let mut port_list = self.port_list.write().unwrap();
        let port = match port_list.get(&id) {
            Some(&port) => port,
            None => {
                let newport = available_port().unwrap();
                port_list.insert(id, newport);
                newport
            }
        };
        // the command must take socket addr at the first parameter
        // followed by the rest of the parameters.

        // Id can be host:port. it is resolved by the server.
        let child = std::process::Command::new(command.name)
            .arg(&format!("http://localhost:{}", port))
            .args(command.args)
            .spawn()
            .expect(&format!("failed to start node id={}", id));

        let mut node_list = self.node_list.write().unwrap();
        node_list.insert(id, Node { child });
    }
    pub fn stop(&self, id: u8) {
        let mut node_list = self.node_list.write().unwrap();
        node_list.remove(&id);
    }
    pub fn pause(&self, id: u8) {
        let mut node_list = self.node_list.write().unwrap();
        node_list.get_mut(&id).unwrap().pause();
    }
    pub fn unpause(&self, id: u8) {
        let mut node_list = self.node_list.write().unwrap();
        node_list.get_mut(&id).unwrap().unpause();
    }
}
impl Drop for Environment {
    fn drop(&mut self) {
        let ids = {
            let node_list = self.node_list.read().unwrap();
            node_list.keys().cloned().collect::<Vec<_>>()
        };
        for id in ids {
            self.stop(id);
        }
    }
}
/// wait for some consensus to be a value computed by `f`.
pub fn wait_for_consensus<T: Eq + Clone>(
    timeout: Duration,
    nodes: Vec<u8>,
    f: impl Fn(u8) -> Option<T>,
) -> Option<T> {
    use std::time::Instant;
    let mut remaining = timeout;
    let mut ok = false;
    let mut ret = None;
    while !ok {
        let start = Instant::now();
        let mut rr = vec![];
        for &id in &nodes {
            let r = f(id);
            rr.push(r);
        }
        ok = true;
        let fi = &rr[0];
        for r in &rr {
            if r.is_none() {
                ok = false;
            }
            if r != fi {
                ok = false;
            }
        }
        if ok {
            let r = rr[0].clone();
            ret = r;
            break;
        }
        let end = Instant::now();
        let elapsed = end - start;
        if remaining < elapsed {
            ret = None;
            break;
        } else {
            remaining -= elapsed;
        }
    }
    ret
}
#[test]
fn test_wait_for_consensus() {
    let mut nodes = vec![];
    for i in 0..100 {
        nodes.push(i);
    }

    let r = wait_for_consensus(Duration::from_secs(1), nodes.clone(), |id| Some(id));
    assert_eq!(r, None);
    let r = wait_for_consensus(Duration::from_secs(1), nodes.clone(), |_| Some(10));
    assert_eq!(r, Some(10));
    let r = wait_for_consensus(Duration::from_secs(1), nodes.clone(), |_| {
        None as Option<u8>
    });
    assert_eq!(r, None);
    let r = wait_for_consensus(Duration::from_secs(1), nodes.clone(), |id| match id {
        0 => Some(0),
        _ => None,
    });
    assert_eq!(r, None);
}
/// wait for every node in `live_nodes` recognize the same membership.
pub fn assert_cluster(timeout: Duration, live_nodes: Vec<u8>, member_nodes: Vec<u8>, env: EnvRef) {
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
/// wait for some value computed by `f` to be `should_be`.
pub fn eventually<T: Eq>(timeout: Duration, should_be: T, f: impl Fn() -> T) -> bool {
    use std::time::Instant;
    let mut remaining = timeout;
    loop {
        let start = Instant::now();
        let res = f();
        if res == should_be {
            return true;
        }
        let end = Instant::now();
        let elapsed = end - start;
        if remaining < elapsed {
            return false;
        }
        remaining -= elapsed;
    }
}
/// make a cluster with the same command.
pub fn make_cluster(n: u8, command: impl Fn(u8) -> NodeCommand) -> EnvRef {
    let env = Environment::new(0, command(0).clone());
    for k in 1..n {
        env.start(k, command(k).clone());
        Admin::to(0, env.clone()).add_server(k);
        let mut nodes = vec![];
        for i in 0..=k {
            nodes.push(i);
        }
        assert_cluster(Duration::from_secs(5), nodes.clone(), nodes, env.clone());
    }
    env
}