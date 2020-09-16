fn main() -> Result<(), Box<dyn std::error::Error>> {
    // let OUT_DIR = "proto_generated";
    // std::fs::create_dir(OUT_DIR);
    // tonic_build::configure()
    //     .format(true)
    //     .out_dir(OUT_DIR)
    //     .compile(&["proto/lol-core.proto"], &["proto"])?;

    tonic_build::compile_protos("proto/lol-core.proto")?;

    Ok(())
}
