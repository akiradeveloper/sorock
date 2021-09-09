FROM 'silkeh/clang:dev'

WORKDIR '/lol_root/codecov'

RUN apt-get update && apt-get install -y \
	curl build-essential

ARG USER
ARG UID
ARG GID
RUN groupadd -g ${GID} ${USER}
RUN useradd -d /home/${USER} -m -s /bin/bash -u ${UID} -g ${GID} -G sudo ${USER}
RUN echo '%wheel ALL=(ALL) NOPASSWD:ALL' >> /etc/sudoers
USER ${USER}

RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
ENV PATH=/home/${USER}/.cargo/bin:$PATH
RUN echo $PATH

ENV CARGO_TARGET_DIR=/tmp/cargo-target
RUN rustup install nightly
RUN rustup default nightly
RUN rustup component add llvm-tools-preview
RUN rustup component add rustfmt
RUN cargo install rustfilt 