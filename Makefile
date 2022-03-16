docker-build:
	docker build -t lol:dev --build-arg USER=${USER} --build-arg UID=`id -u` --build-arg GID=`id -g` - < Dockerfile

.PHONY: install
install:
	cargo install --path examples/kvs --bin kvs-server
	cargo install --path examples/kvs --bin kvs-client
	cargo install --path lol-admin

test: install
	cargo test

bench: install
	cargo +nightly bench