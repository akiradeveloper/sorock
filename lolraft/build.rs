fn main() -> Result<(), Box<dyn std::error::Error>> {
    if std::env::var("DOCS_RS").is_ok() {
        return Ok(());
    }

    let mut config = prost_build::Config::new();
    config.bytes(&[
        ".lolraft.WriteRequest.message",
        ".lolraft.ReadRequest.message",
        ".lolraft.Response.message",
        ".lolraft.KernRequest.message",
        ".lolraft.ReplicationStreamEntry.command",
        ".lolraft.SnapshotChunk.data",
    ]);

    tonic_build::configure()
        .out_dir("src/proto")
        .compile_with_config(config, &["lolraft.proto"], &["proto"])?;

    Ok(())
}
