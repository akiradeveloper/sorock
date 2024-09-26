use anyhow::Result;
use serial_test::serial;

#[serial]
#[tokio::test(flavor = "multi_thread")]
async fn create() -> Result<()> {
    let mut env = env::Env::new();
    env.add_node(0, 1);
    env.check_connectivity(0).await?;
    Ok(())
}

#[serial]
#[tokio::test(flavor = "multi_thread")]
async fn create_remove() -> Result<()> {
    let mut env = env::Env::new();
    env.add_node(0, 1);
    env.check_connectivity(0).await?;

    let mut cli = env.connect_ping_client(0).await?;
    cli.ping(()).await?;

    env.remove_node(0);

    Ok(())
}

#[serial]
#[tokio::test(flavor = "multi_thread")]
async fn panic_loop() -> Result<()> {
    let mut env = env::Env::new();
    env.add_node(0, 1);
    env.check_connectivity(0).await?;

    for _ in 0..10 {
        let mut cli = env.connect_ping_client(0).await?;
        cli.panic(()).await.ok();
    }

    Ok(())
}

#[serial]
#[tokio::test(flavor = "multi_thread")]
async fn drop_env() -> Result<()> {
    for _ in 0..10 {
        let mut env = env::Env::new();
        env.add_node(0, 1);
        env.check_connectivity(0).await?;
    }

    Ok(())
}
