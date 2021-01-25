use super::cluster::*;
use super::env::*;

use std::sync::Arc;

pub type Result<T> = anyhow::Result<T>;
pub type EnvRef = Arc<Environment>;

pub fn kvs_server(args: Vec<&str>) -> NodeCommand {
    NodeCommand::new("kvs-server").with_args(args)
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
pub struct Client {
    to: String,
}
impl Client {
    pub fn to(id: u8, env: EnvRef) -> Self {
        Self {
            to: env.get_node_id(id),
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
pub fn init_cluster(n: u8) -> EnvRef {
    make_cluster(n, |_| kvs_server(vec![]))
}
