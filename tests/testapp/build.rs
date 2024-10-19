fn main() {
    tonic_build::configure()
        .compile_protos(&["ping.proto"], &["proto"])
        .unwrap();
}
