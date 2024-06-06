FROM 'rust:1.78-bookworm'

WORKDIR '/work'

RUN rustup install nightly

RUN apt-get update && apt-get install -y \
	ruby clang \
	build-essential \
	iputils-ping net-tools sudo less \
	cmake \
	protobuf-compiler

RUN curl -sSL "https://github.com/fullstorydev/grpcurl/releases/download/v1.8.9/grpcurl_1.8.9_linux_x86_64.tar.gz" | tar -xz -C /usr/local/bin

RUN cargo install --locked tokio-console