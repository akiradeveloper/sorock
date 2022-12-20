.PHONY: install
install:
	cargo install --path examples/kvs --bin kvs-server
	cargo install --path examples/kvs --bin kvs-client
	cargo install --path lol-admin

test: install
	cargo test

bench: install
	cargo +nightly bench