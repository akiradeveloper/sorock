use std::path::PathBuf;

fn main() {
    let root_dir = PathBuf::from(std::env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .join("sorock");

    let out_dir = root_dir.join("src/generated");
    let file_descriptor_set_path = out_dir.join("sorock_descriptor.bin");
    let ifiles = [
        root_dir.join("proto/sorock.proto"),
        root_dir.join("proto-ext/sorock_monitor.proto"),
    ];
    let include_dirs = [root_dir.join("proto"), root_dir.join("proto-ext")];

    let mut config = prost_build::Config::new();
    config.bytes(&[
        ".sorock.WriteRequest.message",
        ".sorock.ReadRequest.message",
        ".sorock.Response.message",
        ".sorock.KernRequest.message",
        ".sorock.ReplicationStreamEntry.command",
        ".sorock.SnapshotChunk.data",
    ]);

    tonic_build::configure()
        .out_dir(out_dir)
        .file_descriptor_set_path(file_descriptor_set_path)
        .compile_with_config(config, &ifiles, &include_dirs)
        .unwrap();
}
