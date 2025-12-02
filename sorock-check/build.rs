fn main() {
    tonic_build::configure()
        .compile_protos(&["sorock.proto"], &["../sorock/proto"])
        .unwrap();
}
