# FROM 'ubuntu:21.04'
FROM 'rust:1.54-bullseye'

WORKDIR '/lol_root/codecov'

ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y \
	llvm \
	curl sudo less \
	build-essential clang

ARG USER
ARG UID
ARG GID
RUN groupadd -g ${GID} ${USER}
RUN useradd -d /home/${USER} -m -s /bin/bash -u ${UID} -g ${GID} -G sudo ${USER}
RUN echo '%wheel ALL=(ALL) NOPASSWD:ALL' >> /etc/sudoers
USER ${USER}

RUN rustup install nightly
RUN rustup default nightly
RUN rustup component add llvm-tools-preview
RUN rustup component add rustfmt
RUN cargo install rustfilt 