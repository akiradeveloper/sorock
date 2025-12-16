use super::*;

#[derive(Args, Debug)]
pub struct CommandArgs {
    #[clap(long, value_name = "NODE", num_args = 1..)]
    node_list: Vec<Uri>,
}

pub async fn run(args: CommandArgs) -> Result<()> {
    let node_list = args.node_list;

    let mut handles = vec![];
    for node in &node_list {
        let from = node;
        let mut to_list = vec![];
        for n in &node_list {
            if n != from {
                to_list.push(n.clone());
            }
        }
        let handle = tokio::spawn(sync_one(from.clone(), to_list));
        handles.push(handle);
    }

    futures::future::try_join_all(handles).await?;
    Ok(())
}

async fn sync_one(from: Uri, to_list: Vec<Uri>) -> Result<()> {
    let mut cli = sorock::RaftClient::connect(from.clone())
        .await
        .inspect_err(|e| eprintln!("failed to connect to source({from}): {e}"))?;
    let mapping = cli.get_shard_mapping(()).await?.into_inner();

    let mut handles = vec![];
    for to in to_list {
        let from = from.clone();
        let mapping = mapping.clone();
        let handle = tokio::spawn(async move {
            if let Ok(mut to_cli) = sorock::RaftClient::connect(to.clone()).await {
                if let Err(e) = to_cli.update_shard_mapping(mapping).await {
                    eprintln!(
                        "failed to update shard mapping from {} to {}: {}",
                        from, to, e
                    );
                }
            }
        });
        handles.push(handle);
    }

    futures::future::try_join_all(handles).await?;
    Ok(())
}
