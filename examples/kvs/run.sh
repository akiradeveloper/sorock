#!/usr/bin/env bash
# just a simple run script to start the servers and allow cleanup

trap "exit" INT TERM ERR
trap "kill 0" EXIT

# start the nodes
########################################################

RUST_LOG=info cargo run --bin kvs-server http://localhost:3000 &
RUST_LOG=info cargo run --bin kvs-server http://localhost:3001 &
RUST_LOG=info cargo run --bin kvs-server http://localhost:3002 &

# buffer for build times and boot times a bit
########################################################

sleep 30

# cluster the nodes
########################################################

# bootstrap the first node to itself, add other nodes
(cd ../../lol-admin/ && cargo run http://localhost:3000 add-server http://localhost:3000 && cargo run http://localhost:3000 add-server http://localhost:3001 && cargo run http://localhost:3000 add-server http://localhost:3002)

wait
