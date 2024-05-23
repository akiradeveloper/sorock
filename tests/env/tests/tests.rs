use anyhow::Result;
use serial_test::serial;
use test_log::test;

#[serial]
#[test(tokio::test)]
async fn connect_docker_daemon() -> Result<()> {
    let _e = env::Env::new()?;
    Ok(())
}

#[serial]
#[test(tokio::test(flavor = "multi_thread"))]
async fn create() -> Result<()> {
    let mut env = env::Env::new()?;
    env.create(0, 1).await?;
    Ok(())
}

#[serial]
#[test(tokio::test(flavor = "multi_thread"))]
async fn start_stop() -> Result<()> {
    let mut env = env::Env::new()?;
    env.create(0, 1).await?;
    env.start(0).await?;
    env.connect_network(0).await?;

    let mut cli = env.connect_ping_client(0).await?;
    cli.ping(()).await?;

    env.stop(0).await?;
    assert!(env.connect_ping_client(0).await.is_err());

    Ok(())
}

#[serial]
#[tokio::test(flavor = "multi_thread")]
async fn panic_loop() -> Result<()> {
    let mut env = env::Env::new()?;
    env.create(0, 1).await?;
    env.start(0).await?;
    env.connect_network(0).await?;

    for i in 0..1000 {
        dbg!(i);
        let mut cli = env.connect_ping_client(0).await?;
        cli.panic(()).await.ok();
    }

    Ok(())
}

#[serial]
#[tokio::test(flavor = "multi_thread")]
async fn drop_env() -> Result<()> {
    for _ in 0..100 {
        let mut env = env::Env::new()?;
        env.create(0, 1).await?;
        env.start(0).await?;
        env.connect_network(0).await?;
    }

    Ok(())
}