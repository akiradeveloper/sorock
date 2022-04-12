FROM 'rust:1.58.0'

WORKDIR '/work'

RUN apt-get update && apt-get install -y \
	ruby clang \
	build-essential \
	iputils-ping net-tools sudo less \
	cmake

ARG USER
ARG UID
ARG GID
RUN groupadd -g ${GID} ${USER}
RUN useradd -d /home/${USER} -m -s /bin/bash -u ${UID} -g ${GID} -G root ${USER}
RUN echo '%wheel ALL=(ALL) NOPASSWD:ALL' >> /etc/sudoers
USER ${USER}

RUN rustup component add rustfmt
RUN rustup install nightly
RUN rustup component add rustfmt --toolchain nightly