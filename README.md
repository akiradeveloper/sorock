# Sorock

[![Crates.io](https://img.shields.io/crates/v/sorock.svg)](https://crates.io/crates/sorock)
[![API doc](https://docs.rs/sorock/badge.svg)](https://docs.rs/sorock)
[![mdBook Documentation](https://img.shields.io/badge/mdBook-Docs-blue?logo=mdbook)](https://akiradeveloper.github.io/sorock/)
![CI](https://github.com/akiradeveloper/sorock/actions/workflows/ci.yml/badge.svg)
[![codecov](https://codecov.io/gh/akiradeveloper/sorock/graph/badge.svg?token=QOUNE81WNS)](https://codecov.io/gh/akiradeveloper/sorock)
[![MIT licensed](https://img.shields.io/badge/license-MIT-blue.svg)](https://github.com/akiradeveloper/sorock/blob/master/LICENSE)

A Multi-Raft implementation in Rust language.

## Features

![](book/src/images/multi-raft.png)

- Supports Multi-Raft. Mutliple Raft processes can coexist in a single OS process so they can share resources efficiently.
  - Tested with 1000 shards per node.
  - Heartbeats in shards are batched to reduce the network overhead.
- Based on [Tonic](https://github.com/hyperium/tonic) and efficient gRPC streaming is exploited in log replication and snapshot.
- Efficient default backend implementation using [redb](https://github.com/cberner/redb).
  - Writes in shards are batched in one transaction. 
- Phi Accrual Failure Detector is used for leader failure detection.
  - The adaptive algorithm allows you to not choose a fixed timeout number before deployment and to deploy Raft node in even geo-distributed environment where the latency between nodes isn't identical.

## Related Projects

- [phi-detector](https://github.com/akiradeveloper/phi-detector): Implementation of Phi Accrual Failure Detector in Rust.

## Author

Name: Akira Hayakawa  
Email: ruby.wktk@gmail.com
