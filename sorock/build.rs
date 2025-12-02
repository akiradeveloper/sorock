use std::path::PathBuf;

fn main() {
    let out_dir = std::env::var("OUT_DIR").unwrap();
    let out_dir = PathBuf::from(out_dir);

    tonic_prost_build::configure()
        .bytes(".sorock.WriteRequest.message")
        .bytes(".sorock.ReadRequest.message")
        .bytes(".sorock.Response.message")
        .bytes(".sorock.KernRequest.message")
        .bytes(".sorock.ReplicationStreamEntry.command")
        .bytes(".sorock.SnapshotChunk.data")
        .file_descriptor_set_path(out_dir.join("sorock_descriptor.bin"))
        .compile_protos(&["proto/sorock.proto"], &["proto"])
        .unwrap();
}
