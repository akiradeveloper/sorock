# Sorock

[![Crates.io](https://img.shields.io/crates/v/sorock.svg)](https://crates.io/crates/sorock)
[![API doc](https://docs.rs/sorock/badge.svg)](https://docs.rs/sorock)
![CI](https://github.com/akiradeveloper/sorock/actions/workflows/ci.yml/badge.svg)
[![codecov](https://codecov.io/gh/akiradeveloper/sorock/graph/badge.svg?token=QOUNE81WNS)](https://codecov.io/gh/akiradeveloper/sorock)
[![MIT licensed](https://img.shields.io/badge/license-MIT-blue.svg)](https://github.com/akiradeveloper/sorock/blob/master/LICENSE)

A Multi-Raft implementation in Rust language.

[Documentation](https://akiradeveloper.github.io/sorock/)

## Features

![](doc/src/images/multi-raft.png)

- Implements all core [Raft](https://raft.github.io/) features for production use.
- Supports Multi-Raft. Mutliple Raft processes can coexist in a single OS process so they can share resources efficiently.
  - Tested with 1000 shards.
  - Batched heartbeat optimization is implemented.
- Based on [Tonic](https://github.com/hyperium/tonic) and efficient gRPC streaming is exploited in log replication and snapshot.
- Phi Accrual Failure Detector is used for leader failure detection. The adaptive algorithm allows you to not choose a fixed timeout number before deployment and makes it possible to deploy Raft node in even geo-distributed environment where the latency between nodes isn't identical.

## Related Projects

- [sorock-monitor](https://github.com/akiradeveloper/sorock-monitor): Monitoring tool to watch the log state in a cluster. Implementing using [ratatui](https://github.com/ratatui/ratatui).
- [phi-detector](https://github.com/akiradeveloper/phi-detector): Implementation of Phi Accrual Failure Detector in Rust.

## Author

Name: Akira Hayakawa  
Email: ruby.wktk@gmail.com
