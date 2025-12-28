use anyhow::Result;
use clap::Args;
use clap::Parser;
use clap::Subcommand;
use futures::Stream;
use futures::StreamExt;
use std::collections::{HashMap, HashSet};
use std::pin::Pin;
use std::time::{Duration, Instant};
use tonic::transport::{Endpoint, Uri};

mod sorock {
    tonic::include_proto!("sorock");
    pub type RaftClient = raft_client::RaftClient<tonic::transport::channel::Channel>;
}

mod sub;

#[derive(Subcommand, Debug)]
enum Sub {
    Sync(sub::sync::CommandArgs),
    Remap(sub::remap::CommandArgs),
    Monitor(sub::monitor::CommandArgs),
}

#[derive(Parser, Debug)]
struct Cli {
    #[command(subcommand)]
    sub: Sub,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Cli::parse();
    match args.sub {
        Sub::Sync(args) => {
            sub::sync::run(args).await?;
        }
        Sub::Remap(args) => {
            sub::remap::run(args)?;
        }
        Sub::Monitor(args) => {
            sub::monitor::run(args).await?;
        }
    }

    Ok(())
}
