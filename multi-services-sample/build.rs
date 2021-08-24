fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = prost_build::Config::new();
    tonic_build::configure().compile_with_config(config, &["proto/my-service.proto"], &["proto"])?;
    Ok(())
}
