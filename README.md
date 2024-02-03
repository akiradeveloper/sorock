# lol

**This project in under complete reworking. Please wait for a while.**

![CI](https://github.com/akiradeveloper/lol/workflows/CI/badge.svg)
[![MIT licensed](https://img.shields.io/badge/license-MIT-blue.svg)](https://github.com/akiradeveloper/lol/blob/master/LICENSE)
[![Tokei](https://tokei.rs/b1/github/akiradeveloper/lol)](https://github.com/akiradeveloper/lol)

A Raft implementation in Rust language. To support this project please give it a ⭐

![](https://user-images.githubusercontent.com/785824/146726060-63b12378-ecb7-49f9-8025-a65dbd37e9b2.jpeg)

## Features

![スクリーンショット 2024-02-03 13 29 55](https://github.com/akiradeveloper/lol/assets/785824/f6a6ceef-98f3-4fcf-9ba8-3655d52bd3f0)


- Implements all fundamental [Raft](https://raft.github.io/) features for production use.
- Supports Multi-Raft. Mutliple Raft processes can coexist in a single OS process so they can share resources efficiently.
- Based on [Tonic](https://github.com/hyperium/tonic) and efficient gRPC streaming is exploited in log replication and snapshot.
- [Phi Accrual Failure Detector](https://github.com/akiradeveloper/phi-detector) is used for leader failure detection. The adaptive algorithm allows you to not choose a fixed timeout number in prior to deployment and makes it possible to deploy Raft node in even Geo-distributed environment.

## Architecture

To implement Multi-Raft, the architecture is divided into two spaces. One in the lower side is called "Pure Raft" layer which is totally unaware of 
gRPC and Multi-Raft. Therefore, called pure. The other side translates gRPC requests into pure requests and vice versa.

![スクリーンショット 2024-02-03 13 30 09](https://github.com/akiradeveloper/lol/assets/785824/fd064ba6-be20-4934-839a-db8cd07a8f13)



## Development

- `docker compose build` to build test servers.
- TERM1: `./log` to start log watcher.
- TERM2: `./dev` to start the dev container.
- TERM2: `cargo test`.

## Author

Akira Hayakawa  
EMail: ruby.wktk@gmail.com
