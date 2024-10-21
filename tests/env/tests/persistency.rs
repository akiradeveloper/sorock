use anyhow::Result;
use std::time::Duration;

#[tokio::test(flavor = "multi_thread")]
async fn create() -> Result<()> {
    let mut env = env::Env::new(1, true, true);
    env.add_node(0);
    env.check_connectivity(0).await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn create_remove() -> Result<()> {
    let mut env = env::Env::new(1, true, true);
    env.add_node(0);
    env.check_connectivity(0).await?;
    let addr0 = env.address(0);
    env.remove_node(0);
    tokio::time::sleep(Duration::from_secs(1)).await;

    env.add_node(0);
    env.check_connectivity(0).await?;
    let addr1 = env.address(0);

    assert_eq!(addr0, addr1);
    Ok(())
}
