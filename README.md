# lol

[![Crates.io](https://img.shields.io/crates/v/lol-core.svg)](https://crates.io/crates/lol-core)
[![documentation](https://docs.rs/lol-core/badge.svg)](https://docs.rs/lol-core)
![CI](https://github.com/akiradeveloper/lol/workflows/CI/badge.svg)
[![MIT licensed](https://img.shields.io/badge/license-MIT-blue.svg)](https://github.com/akiradeveloper/lol/blob/master/LICENSE)
[![Tokei](https://tokei.rs/b1/github/akiradeveloper/lol)](https://github.com/akiradeveloper/lol)

A Raft implementation in Rust language. To support this project please give it a ⭐

[Get Started](https://github.com/akiradeveloper/lol/wiki)

## How to participate in this project

Use docker container to make an dev environment on your computer.

- `make docker-build` to build the docker image
- `./dev` to start the container

then

- `cargo build` to compile the whole project
- `make test` to run the regression tests
- `make bench` to run the benchmark