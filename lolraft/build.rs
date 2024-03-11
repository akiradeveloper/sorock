fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut config = prost_build::Config::new();
    config.bytes(&[
        ".lolraft.WriteRequest.message",
        ".lolraft.ReadRequest.message",
        ".lolraft.Response.message",
        ".lolraft.KernRequest.message",
        ".lolraft.ReplicationStreamEntry.command",
        ".lolraft.SnapshotChunk.data",
    ]);

    tonic_build::configure().out_dir("proto").compile_with_config(config, &["lolraft.proto"], &["proto"])?;

    Ok(())
}
