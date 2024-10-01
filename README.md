# sorock

[![Crates.io](https://img.shields.io/crates/v/sorock.svg)](https://crates.io/crates/sorock)
[![API doc](https://docs.rs/sorock/badge.svg)](https://docs.rs/sorock)
![CI](https://github.com/akiradeveloper/sorock/actions/workflows/ci.yml/badge.svg)
[![MIT licensed](https://img.shields.io/badge/license-MIT-blue.svg)](https://github.com/akiradeveloper/sorock/blob/master/LICENSE)

A Multi-Raft implementation in Rust language.

[Documentation](https://akiradeveloper.github.io/sorock/)

![146726060-63b12378-ecb7-49f9-8025-a65dbd37e9b2](https://github.com/akiradeveloper/sorock/assets/785824/12a016fe-35a0-4d12-8ffa-955ef61b25b9)

## Features

![](doc/src/images/multi-raft.png)

- Implements all core [Raft](https://raft.github.io/) features for production use.
- Supports Multi-Raft. Mutliple Raft processes can coexist in a single OS process so they can share resources efficiently.
  - Tested with 1000 shards.
  - Batched heartbeat optimization is implemented.
- Based on [Tonic](https://github.com/hyperium/tonic) and efficient gRPC streaming is exploited in log replication and snapshot.
- [Phi Accrual Failure Detector](https://github.com/akiradeveloper/phi-detector) is used for leader failure detection. The adaptive algorithm allows you to not choose a fixed timeout number in prior to deployment and makes it possible to deploy Raft node in even geo-distributed environment.

## Author

Akira Hayakawa  
EMail: ruby.wktk@gmail.com
