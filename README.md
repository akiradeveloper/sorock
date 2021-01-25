# lol

[![Crates.io](https://img.shields.io/crates/v/lol-core.svg)](https://crates.io/crates/lol-core)
[![documentation](https://docs.rs/lol-core/badge.svg)](https://docs.rs/lol-core)
![CI](https://github.com/akiradeveloper/lol/workflows/CI/badge.svg)
[![MIT licensed](https://img.shields.io/badge/license-MIT-blue.svg)](https://github.com/akiradeveloper/lol/blob/master/LICENSE)
[![Tokei](https://tokei.rs/b1/github/akiradeveloper/lol)](https://github.com/akiradeveloper/lol)

A Raft implementation in Rust language. To support this project please give it a ⭐

[Documentation (mdBook)](https://akiradeveloper.github.io/lol/)

## Features

- Implements all basic [Raft](https://raft.github.io/) features: Replication, Leader Election, Log Compaction, Persistency, Dynamic Membership Change, Streaming Snapshot, etc.
- Based on [Tonic](https://github.com/hyperium/tonic) and efficient gRPC streaming is fully utilized in log replication and snapshot copying.
- [Phi Accrual Failure Detector](https://www.computer.org/csdl/proceedings-article/srds/2004/22390066/12OmNvT2phv) is used in leader failure detection. This adaptive algorithm lets you not choose a fixed timeout number before deployment and makes it possible to deploy Raft node in Geo-distributed environment. This algorithm is also used in [Akka](https://akka.io/).
- Clear Abstractions: RaftApp is your application or state machine in Raft's context. RaftStorage is the abstraction of the backend storage with which both in-memory and persistent (backed by RocksDB) are supported.

## Example

```rust
// Implement RaftApp for YourApp!
struct YourApp { ... }
impl RaftApp for YourApp {
    ...
}
let app = YourApp { ... };
let storage = ...; // Choose a backend from lol_core::storage
let core = RaftCore::new(app, storage, config, ...);
let service = lol_core::make_service(core);
tonic::transport::Server::builder()
  .add_service(service)
  .serve(socket).await;
```

## Development

Use docker container to make an dev environment on your computer.

- `make docker-build` to build the docker image
- `./dev` to start the container

then

- `cargo build` to compile the whole project
- `make test` to run the regression tests
- `make bench` to run the benchmark