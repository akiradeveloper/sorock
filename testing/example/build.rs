fn main() {
    tonic_prost_build::configure()
        .compile_protos(&["ping.proto"], &["proto"])
        .unwrap();
}
