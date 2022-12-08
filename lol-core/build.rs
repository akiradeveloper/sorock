fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut config = prost_build::Config::new();
    config.bytes(&[
        ".lol_core.AppendStreamEntry.command",
        ".lol_core.GetSnapshotRep.chunk",
    ]);
    // Output the generated rs files to `src/proto/`
    tonic_build::configure()
        .out_dir("src/proto/")
        .compile_with_config(config, &["lol_core.proto"], &["proto"])?;

    Ok(())
}
