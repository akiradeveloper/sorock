FROM 'rust:1.65.0'

WORKDIR '/work'

RUN apt-get update && apt-get install -y \
	ruby clang \
	build-essential \
	iputils-ping net-tools sudo less \
	cmake

# Tonic 0.8 requires protoc to be installed.
# https://github.com/hyperium/tonic/issues/1049
RUN mkdir /protoc
RUN wget https://github.com/protocolbuffers/protobuf/releases/download/v21.4/protoc-21.4-linux-$(uname -m).zip -P /protoc -O a.zip
RUN unzip a.zip -d /protoc
ENV PATH $PATH:/protoc/bin

RUN rustup component add rustfmt
RUN rustup install nightly
RUN rustup component add rustfmt --toolchain nightly