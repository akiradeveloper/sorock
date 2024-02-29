FROM 'rust:1.76-bookworm'

WORKDIR '/work'

RUN apt-get update && apt-get install -y \
	ruby clang \
	build-essential \
	iputils-ping net-tools sudo less \
	cmake \
	protobuf-compiler