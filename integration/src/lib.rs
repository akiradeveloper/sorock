use std::collections::{HashMap, HashSet};
use std::net::TcpListener;
use std::process::Child;
use std::sync::{Arc, RwLock};
use std::thread;
use std::time::Duration;

pub type Result<T> = anyhow::Result<T>;
pub type EnvRef = Arc<Environment>;

pub struct Client {
    to: String,
}
impl Client {
    pub fn to(id: u8, env: EnvRef) -> Self {
        Self {
            to: env.node_id(id),
        }
    }
    fn command(&self) -> Command {
        Command::new("kvs-client").arg(&self.to)
    }
    pub fn set(&self, k: &str, v: &str) -> Result<()> {
        self.command().args(&["set", k, v]).run().map(|_| ())
    }
    pub fn set_rep(&self, k: &str, v: &str, rep: u32) -> Result<()> {
        self.command()
            .args(&["set", &format!("--rep={}", rep), k, v])
            .run()
            .map(|_| ())
    }
    pub fn get(&self, k: &str) -> Result<kvs::Get> {
        self.command()
            .args(&["get", k])
            .run()
            .map(|buf| serde_json::from_slice::<kvs::Get>(&buf).unwrap())
    }
    pub fn list(&self) -> Result<kvs::List> {
        self.command()
            .arg("list")
            .run()
            .map(|buf| serde_json::from_slice::<kvs::List>(&buf).unwrap())
    }
}
pub struct Admin {
    to: String,
}
impl Admin {
    pub fn to(id: u8, env: EnvRef) -> Self {
        Self {
            to: env.node_id(id),
        }
    }
    fn command(&self) -> Command {
        Command::new("lol-admin").arg(&self.to)
    }
    pub fn add_server(&self, id: u8, env: EnvRef) -> Result<()> {
        self.command()
            .arg("add-server")
            .arg(&env.node_id(id))
            .run()
            .map(|_| ())
    }
    pub fn remove_server(&self, id: u8, env: EnvRef) -> Result<()> {
        self.command()
            .arg("remove-server")
            .arg(&env.node_id(id))
            .run()
            .map(|_| ())
    }
    fn init_cluster(&self) -> Result<lol_admin::InitCluster> {
        self.command()
            .arg("init-cluster")
            .run()
            .map(|buf| serde_json::from_slice::<lol_admin::InitCluster>(&buf).unwrap())
    }
    pub fn cluster_info(&self) -> Result<lol_admin::ClusterInfo> {
        self.command()
            .arg("cluster-info")
            .run()
            .map(|buf| serde_json::from_slice::<lol_admin::ClusterInfo>(&buf).unwrap())
    }
    pub fn timeout_now(&self) -> Result<()> {
        self.command().arg("timeout-now").run().map(|_| ())
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
        Command::new("kill")
            .arg("-STOP")
            .arg(&format!("{}", self.pid()))
            .run();
    }
    fn unpause(&mut self) {
        Command::new("kill")
            .arg("-CONT")
            .arg(&format!("{}", self.pid()))
            .run();
    }
}
impl Drop for Node {
    fn drop(&mut self) {
        self.child.kill();
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
    pub fn new(id: u8) -> Arc<Self> {
        thread::sleep(Duration::from_secs(1));
        let x = Self {
            port_list: RwLock::new(HashMap::new()),
            node_list: RwLock::new(HashMap::new()),
        };
        x.start(id);
        thread::sleep(Duration::from_millis(1000));
        let env = Arc::new(x);
        Admin::to(id, env.clone()).init_cluster().unwrap();
        thread::sleep(Duration::from_millis(2000));
        env
    }
    pub fn node_id(&self, id: u8) -> String {
        let port_list = self.port_list.read().unwrap();
        let port = port_list.get(&id).unwrap();
        format!("127.0.0.1:{}", port)
    }
    pub fn start(&self, id: u8) {
        let mut port_list = self.port_list.write().unwrap();
        let port = match port_list.get(&id) {
            Some(&port) => port,
            None => {
                let newport = available_port().unwrap();
                port_list.insert(id, newport);
                newport
            }
        };
        let child = std::process::Command::new("kvs-server")
            .arg(&format!("127.0.0.1:{}", port))
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
struct Command {
    name: String,
    args: Vec<String>,
}
impl Command {
    fn new(name: &str) -> Self {
        Self {
            name: name.to_owned(),
            args: vec![],
        }
    }
    fn arg(mut self, x: &str) -> Self {
        self.args.push(x.to_owned());
        self
    }
    fn args(mut self, xs: &[&str]) -> Self {
        for x in xs {
            self = self.arg(x);
        }
        self
    }
    fn run(self) -> Result<Vec<u8>> {
        use std::io::Write;
        use std::process::Command;
        let mut command = Command::new(&self.name);
        for x in self.args {
            command.arg(&x);
        }
        let res = command.output().expect("failed to execute command");
        std::io::stdout().write_all(&res.stderr).unwrap();
        if res.status.success() {
            Ok(res.stdout)
        } else {
            Err(anyhow::anyhow!("failed to output"))
        }
    }
}
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
pub fn assert_cluster(timeout: Duration, live_nodes: Vec<u8>, member_nodes: Vec<u8>, env: EnvRef) {
    let mut leader_can_be = HashSet::new();
    for &id in &live_nodes {
        leader_can_be.insert(Some(env.node_id(id)));
    }
    let mut membership_should_be = vec![];
    for id in member_nodes {
        membership_should_be.push(env.node_id(id));
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
pub fn init_cluster(n: u8) -> Arc<Environment> {
    let env = Environment::new(0);
    for k in 1..n {
        env.start(k);
        Admin::to(0, env.clone()).add_server(k, env.clone());
        let mut nodes = vec![];
        for i in 0..=k {
            nodes.push(i);
        }
        assert_cluster(Duration::from_secs(5), nodes.clone(), nodes, env.clone());
    }
    env
}
