use super::*;

use sorock::raft_client::RaftClient;
use std::io::BufRead;

mod calc;
mod manipulator;
mod parse;

#[derive(Args, Debug)]
pub struct CommandArgs {
    #[clap(long, value_name = "NODE", num_args = 1..)]
    node_list: Vec<Uri>,

    #[clap(short, long, value_name = "INTERVAL", default_value = "5s", value_parser = humantime::parse_duration)]
    interval: Duration,
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

pub fn run(args: CommandArgs) -> Result<()> {
    let node_list = args.node_list;
    let interval = args.interval;
    let parser = parse::ParseLine::new(node_list.clone());

    for line in std::io::stdin().lock().lines() {
        let line = line?;
        let parsed = parser.parse(&line).expect("failed to parse line");

        // Manipulate all shards in parallel.
        tokio::spawn(async move {
            let mut manipulator = manipulator::Manipulator::new(parsed.shard_id, parsed.replicas);
            let mut interval = tokio::time::interval(interval);
            loop {
                interval.tick().await;
                manipulator.run_once().await.ok();
            }
        });
    }

    Ok(())
}
