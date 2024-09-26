use std::path::PathBuf;

fn main() {
    let root_dir = PathBuf::from(std::env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .join("lol");

    let out_dir = root_dir.join("src/generated");
    let file_descriptor_set_path = out_dir.join("lol_descriptor.bin");
    let ifiles = [root_dir.join("proto/lol.proto")];
    let include_dirs = [root_dir.join("proto")];

    let mut config = prost_build::Config::new();
    config.bytes(&[
        ".lol.WriteRequest.message",
        ".lol.ReadRequest.message",
        ".lol.Response.message",
        ".lol.KernRequest.message",
        ".lol.ReplicationStreamEntry.command",
        ".lol.SnapshotChunk.data",
    ]);

    tonic_build::configure()
        .out_dir(out_dir)
        .file_descriptor_set_path(file_descriptor_set_path)
        .compile_with_config(config, &ifiles, &include_dirs)
        .unwrap();
}
