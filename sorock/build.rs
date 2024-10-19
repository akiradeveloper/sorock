use std::path::PathBuf;
use protox::prost::Message;

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

    let fds = protox::compile(ifiles, include_dirs).unwrap();
    std::fs::write(&fd_path, fds.encode_to_vec()).unwrap();

    tonic_build::configure()
        .out_dir(out_dir)
        .compile_fds_with_config(config, fds)
        .unwrap();
}
