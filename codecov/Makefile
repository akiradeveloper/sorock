LOL_ROOT=..

docker-build:
	docker build -t lol:cov --build-arg USER=${USER} --build-arg UID=`id -u` - < Dockerfile

install:
	cd ${LOL_ROOT} && cargo install --path lol-admin
	cd ${LOL_ROOT} && cargo install --path examples/kvs --bin kvs-client
	# We only concern the code coverage by kvs-server, not by clients.
	cd ${LOL_ROOT} && RUSTFLAGS="-Zinstrument-coverage" cargo +nightly install --debug --path examples/kvs --bin kvs-server

clean_cov_files:
	cd ${LOL_ROOT} && rm -rf integration-tests/cov

test: clean_cov_files install
	cd ${LOL_ROOT} && cargo test -- --test-threads=1

cov_report:
	docker-compose run cov sh script/cov_report.sh