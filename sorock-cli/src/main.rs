use anyhow::Result;
use clap::Args;
use clap::Parser;
use clap::Subcommand;
use tonic::transport::Uri;

mod sorock {
    tonic::include_proto!("sorock");
    pub type RaftClient = raft_client::RaftClient<tonic::transport::channel::Channel>;
}

mod sub;

#[derive(Subcommand, Debug)]
enum Sub {
    Sync(sub::sync::CommandArgs),
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
    }

    Ok(())
}
