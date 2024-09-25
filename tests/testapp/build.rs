fn main() {
    tonic_build::configure()
        .compile(&["ping.proto"], &["proto"])
        .unwrap();
}
