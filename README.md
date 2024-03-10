# lol

![CI](https://github.com/akiradeveloper/lol/workflows/CI/badge.svg)
[![MIT licensed](https://img.shields.io/badge/license-MIT-blue.svg)](https://github.com/akiradeveloper/lol/blob/master/LICENSE)

A Multi-Raft implementation in Rust language.

![146726060-63b12378-ecb7-49f9-8025-a65dbd37e9b2](https://github.com/akiradeveloper/lol/assets/785824/12a016fe-35a0-4d12-8ffa-955ef61b25b9)

## Features

![スクリーンショット 2024-03-11 7 54 06](https://github.com/akiradeveloper/lol/assets/785824/ea538adc-54a1-4a86-9712-1cd98da00958)

- Implements all core [Raft](https://raft.github.io/) features for production use.
- Supports Multi-Raft. Mutliple Raft processes can coexist in a single OS process so they can share resources efficiently.
- Based on [Tonic](https://github.com/hyperium/tonic) and efficient gRPC streaming is exploited in log replication and snapshot.
- [Phi Accrual Failure Detector](https://github.com/akiradeveloper/phi-detector) is used for leader failure detection. The adaptive algorithm allows you to not choose a fixed timeout number in prior to deployment and makes it possible to deploy Raft node in even geo-distributed environment.

## Architecture

To implement Multi-Raft, the architecture is split into two spaces. One in the lower side is called "Pure Raft" layer which is totally unaware of 
gRPC and Multi-Raft. Therefore, it is called pure. The other side translates gRPC requests into pure requests and vice versa.

![スクリーンショット 2024-03-11 8 00 03](https://github.com/akiradeveloper/lol/assets/785824/28fd68c4-6aa9-4bc8-a1ac-c44eb8563751)

## Development

- `docker compose build` to build test application.
- TERM1: `./log` to start log watcher.
- TERM2: `./dev` to start the dev container.
- TERM2: `cargo test`.

## Author

Akira Hayakawa  
EMail: ruby.wktk@gmail.com
