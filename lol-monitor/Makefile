.PHONY: install
install:
	cd ..; cargo install --path lol-admin
	cd ..; cargo install --path examples/kvs --bin kvs-client
	# We only concern the code coverage by kvs-server, not by clients.
	cd ..; cargo install --debug --path examples/kvs --bin kvs-server

test: install
	ruby test.rb