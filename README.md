# lol

[![Crates.io](https://img.shields.io/crates/v/lol-core.svg)](https://crates.io/crates/lol-core)
[![documentation](https://docs.rs/lol-core/badge.svg)](https://docs.rs/lol-core)
![CI](https://github.com/akiradeveloper/lol/workflows/CI/badge.svg)
[![MIT licensed](https://img.shields.io/badge/license-MIT-blue.svg)](https://github.com/akiradeveloper/lol/blob/master/LICENSE)
[![Tokei](https://tokei.rs/b1/github/akiradeveloper/lol)](https://github.com/akiradeveloper/lol)

A Raft implementation in Rust language. To support this project please give it a ⭐

[Documentation](https://akiradeveloper.github.io/lol/)

![](https://user-images.githubusercontent.com/785824/146726060-63b12378-ecb7-49f9-8025-a65dbd37e9b2.jpeg)

## Features

- Implements all basic [Raft](https://raft.github.io/) features: Replication, Leader Election, Log Compaction, Persistency, Dynamic Membership Change, Streaming Snapshot, etc.
- Based on [Tonic](https://github.com/hyperium/tonic) and efficient gRPC streaming is fully utilized in log replication and snapshot copying.
- [Phi Accrual Failure Detector](https://www.computer.org/csdl/proceedings-article/srds/2004/22390066/12OmNvT2phv) is used in leader failure detection. This adaptive algorithm lets you not choose a fixed timeout number before deployment and makes it possible to deploy Raft node in Geo-distributed environment. This algorithm is also used in [Akka](https://akka.io/).
- Clear Abstractions: RaftApp is your application or state machine in Raft's context. RaftStorage is the abstraction of the backend storage with which both in-memory and persistent (backed by RocksDB) are supported.

## Example

```
[dependencies]
lol-core = "0.8"
```

```rust
// Implement RaftApp for YourApp!
struct YourApp { ... }
impl RaftApp for YourApp {
    ...
}
// Initialize your app.
let app = YourApp { ... };
// Choose a backend.
let storage = storage::memory::Storage::new();
// This is the Id of this node.
let uri = "https://192.168.10.15:50000".parse().unwrap();
let config = ConfigBuilder::default().build().unwrap();
// Make a tower::Service.
let service = make_raft_service(app, storage, uri, config);
// Start a gRPC server with the service.
tonic::transport::Server::builder()
    .add_service(service)
    .serve(socket).await;
```

## Related Projects

- [lol-perf](https://github.com/akiradeveloper/lol-perf): Performance analysis project which utilizes [cargo-flamegraph](https://github.com/flamegraph-rs/flamegraph)
and [cargo-profiler](https://github.com/svenstaro/cargo-profiler).
- [phi-detector](https://github.com/akiradeveloper/phi-detector): Implementation of Phi Accrual Failure Detector.

## Development

Use docker container to make an dev environment on your computer.

- `make` to build the docker image
- `./dev` to start the dev container

then

- `cargo build` to compile the entire project
- `make test` to run the regression tests
- `make bench` to run the benchmark tests
