fn main() {
    let fds = protox::compile(&["ping.proto"], &["proto"]).unwrap();
    tonic_build::configure()
        .compile_fds(fds)
        .unwrap();
}
