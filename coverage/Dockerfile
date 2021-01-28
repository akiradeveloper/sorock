from 'ubuntu:21.04'
WORKDIR '/lol-root/coverage'

ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update
RUN apt-get install -y llvm
RUN apt-get install -y curl sudo less
RUN apt-get install -y build-essential

ARG USER
ARG UID
RUN groupadd ${USER}
RUN useradd -d /home/${USER} -m -s /bin/bash -u ${UID} -g ${USER} -G sudo ${USER}
USER ${USER}

RUN curl https://sh.rustup.rs -sSf >> ${HOME}/rustup.rs
RUN sh ${HOME}/rustup.rs -y
RUN echo $HOME
ENV PATH=/home/${USER}/.cargo/bin:$PATH
RUN echo $PATH

RUN rustup install nightly
RUN rustup component add --toolchain nightly llvm-tools-preview
RUN cargo install rustfilt 