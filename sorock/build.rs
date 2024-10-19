use std::path::PathBuf;

fn main() {
    let out_dir = std::env::var("OUT_DIR").unwrap();
    let out_dir = PathBuf::from(out_dir);

    let ifiles = ["proto/sorock.proto", "proto-ext/sorock_monitor.proto"];
    let include_dirs = ["proto", "proto-ext"];
    let fd_path = out_dir.join("sorock_descriptor.bin");

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
        .file_descriptor_set_path(fd_path)
        .compile_protos_with_config(config, &ifiles, &include_dirs)
        .unwrap();
}
