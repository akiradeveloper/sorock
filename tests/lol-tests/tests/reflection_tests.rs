use anyhow::Result;
use lol_tests::*;
use serial_test::serial;

#[serial]
#[tokio::test(flavor = "multi_thread")]
async fn reflection_grpcurl_access() -> Result<()> {
    let mut cluster = Cluster::new(1, 1).await?;

    let address = {
        let raw: tonic::transport::Uri = cluster.env().address(0);
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
