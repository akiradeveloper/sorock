fn main() {
    tonic_prost_build::configure()
        .compile_protos(&["sorock.proto"], &["../sorock/proto"])
        .unwrap();
}
