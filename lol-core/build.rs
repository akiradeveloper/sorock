fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut config = prost_build::Config::new();
    config.bytes(&[
        ".lol_core.AppendStreamEntry.command",
        ".lol_core.GetSnapshotRep.chunk",
    ]);
    tonic_build::configure().compile_with_config(config, &["proto/lol_core.proto"], &["proto"])?;

    Ok(())
}
