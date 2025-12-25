use anyhow::Result;
use sorock_tests::*;

#[tokio::test(flavor = "multi_thread")]
async fn reflection_grpcurl_access() -> Result<()> {
    let cluster = Cluster::new(1, 1).await?;

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
