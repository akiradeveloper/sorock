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
    env.create(0).await?;
    Ok(())
}

#[serial]
#[test(tokio::test(flavor = "multi_thread"))]
async fn start_stop() -> Result<()> {
    let mut env = env::Env::new()?;
    env.create(0).await?;
    env.start(0).await?;
    env.connect_network(0).await?;

    env.ping(0).await?;

    env.stop(0).await?;
    assert!(env.ping(0).await.is_err());

    Ok(())
}
