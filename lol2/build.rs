fn main() -> Result<(), Box<dyn std::error::Error>> {
    if std::env::var("DOCS_RS").is_ok() {
        return Ok(());
    }

    let mut config = prost_build::Config::new();
    config.bytes(&[
        ".lol2.WriteRequest.message",
        ".lol2.ReadRequest.message",
        ".lol2.Response.message",
        ".lol2.KernRequest.message",
        ".lol2.ReplicationStreamEntry.command",
        ".lol2.SnapshotChunk.data",
    ]);

    tonic_build::configure().compile_with_config(config, &["lol2.proto"], &["proto"])?;

    Ok(())
}
