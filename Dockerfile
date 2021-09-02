FROM 'rust:1.54.0'
WORKDIR '/work'
RUN rustup component add rustfmt
RUN rustup install nightly
RUN rustup component add rustfmt --toolchain nightly
RUN apt-get update && \
	apt-get install -y ruby clang && \
	apt-get install -y build-essential && \
	apt-get install -y iputils-ping net-tools sudo

ARG USER
ARG UID
ARG GID
RUN groupadd -g ${GID} ${USER}
RUN useradd -d /home/${USER} -m -s /bin/bash -u ${UID} -g ${GID} -G root ${USER}
RUN echo '%wheel ALL=(ALL) NOPASSWD:ALL' >> /etc/sudoers
USER ${USER}