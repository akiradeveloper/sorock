docker-build:
	docker build -t lol:cov --build-arg USER=${USER} --build-arg UID=`id -u` --build-arg GID=`id -g` - < Dockerfile

report:
	sh script/cov_report.sh

.PHONY: install
install:
	cd ..; cargo install --path lol-admin
	cd ..; cargo install --path examples/kvs --bin kvs-client
	# We only concern the code coverage by kvs-server, not by clients.
	cd ..; RUSTFLAGS="-Zinstrument-coverage" cargo install --debug --path examples/kvs --bin kvs-server

.PHONY: install
clean_cov_files:
	cd ..; rm -rf integration-tests/cov

test: clean_cov_files install
	cd ..; cargo test -- --test-threads=1