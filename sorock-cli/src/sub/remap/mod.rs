use super::*;

use clap::parser;
use sorock::raft_client::RaftClient;

mod calc;
mod manipulator;
mod parse;

#[derive(Args, Debug)]
pub struct CommandArgs {
    #[clap(long, value_name = "NODE", num_args = 1..)]
    node_list: Vec<Uri>,
}

#[derive(Debug)]
struct ShardState {
    h: HashMap<Uri, calc::State>,
}

impl ShardState {
    fn pick_one_node(&self) -> Uri {
        self.h.keys().next().unwrap().clone()
    }
}

#[tokio::main]
pub async fn run(args: CommandArgs) -> Result<()> {
    let node_list = args.node_list;
    let parser = parse::ParseLine::new(node_list.clone());

    Ok(())
}
