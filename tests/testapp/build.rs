fn main() {
    tonic_build::configure()
        .compile(&["testapp.proto"], &["proto"])
        .unwrap();
}
