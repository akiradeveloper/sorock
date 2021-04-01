fn main() -> Result<(), Box<dyn std::error::Error>> {
    // let OUT_DIR = "proto_generated";
    // std::fs::create_dir(OUT_DIR);
    // tonic_build::configure()
    //     .format(true)
    //     .out_dir(OUT_DIR)
    //     .compile(&["proto/lol-core.proto"], &["proto"])?;

    let mut config = prost_build::Config::new();
    config.bytes(&[".lol_core.AppendStreamEntry.command"]);
    config.protoc_arg("--experimental_allow_proto3_optional");
    tonic_build::configure().compile_with_config(config, &["proto/lol-core.proto"], &["proto"])?;

    Ok(())
}
