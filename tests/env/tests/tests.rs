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

    env.start(0).await?;
    env.ping(0).await?;

    Ok(())
}

#[ignore]
#[serial]
#[test(tokio::test(flavor = "multi_thread"))]
async fn pause_resume() -> Result<()> {
    let mut env = env::Env::new()?;
    env.create(0).await?;
    env.start(0).await?;
    env.connect_network(0).await?;
    env.ping(0).await?;

    env.pause(0).await?;
    // hang
    assert!(env.ping(0).await.is_err());

    env.resume(0).await?;
    env.ping(0).await?;

    Ok(())
}

#[ignore]
#[serial]
#[test(tokio::test(flavor = "multi_thread"))]
async fn pause_resume_v2() -> Result<()> {
    let mut env = env::Env::new()?;
    env.create(0).await?;
    env.start(0).await?;
    env.connect_network(0).await?;
    env.ping(0).await?;

    env.pause_v2(0).await?;
    assert!(env.ping(0).await.is_err());

    env.resume_v2(0).await?;
    env.ping(0).await?;

    Ok(())
}

#[serial]
#[test(tokio::test(flavor = "multi_thread"))]
async fn connect_disconnect_network() -> Result<()> {
    let mut env = env::Env::new()?;
    env.create(0).await?;

    env.connect_network(0).await?;
    assert!(env.ping(0).await.is_err());

    env.start(0).await?;
    env.ping(0).await?;

    env.disconnect_network(0).await?;
    // hang
    // cli.ping(()).await?;

    Ok(())
}

#[serial]
#[test(tokio::test(flavor = "multi_thread"))]
async fn ping_connectivity() -> Result<()> {
    let mut env = env::Env::new()?;
    env.create(0).await?;

    let chan = env.connect(0);
    assert!(env.ping(0).await.is_err());

    env.start(0).await?;
    assert!(env.ping(0).await.is_err());

    env.connect_network(0).await?;
    env.ping(0).await?;

    // hang
    // env.disconnect_network(0).await?;
    // assert!(cli.ping(()).await.is_err());

    // env.connect_network(0).await?;
    // assert!(cli.ping(()).await.is_ok());

    env.stop(0).await?;
    assert!(env.ping(0).await.is_err());

    Ok(())
}
