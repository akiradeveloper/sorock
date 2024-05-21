use anyhow::Result;
use lol_tests::*;
use serial_test::serial;
use test_log::test;

#[serial]
#[test(tokio::test(flavor = "multi_thread"))]
async fn reflection_grpcurl_access() -> Result<()> {
    let _cluster = Cluster::new(1, 1).await?;

    let address = {
        let raw: tonic::transport::Uri = env::address_from_id(0).parse()?;
        let out = format!("{}", raw.authority().unwrap());
        dbg!(&out);
        out
    };

    let out = std::process::Command::new("grpcurl")
        .arg("-plaintext")
        .arg(address)
        .arg("list")
        .output()?;

    assert!(out.status.success());
    dbg!(String::from_utf8(out.stdout)?);

    Ok(())
}
