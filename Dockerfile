FROM 'centos:7'
WORKDIR '/lol-root'

RUN yum install -y sudo gcc
RUN yum install -y iputils bind-utils make
RUN yum install -y ruby

ARG USER
ARG UID
RUN groupadd ${USER}
RUN useradd -d /home/${USER} -m -s /bin/bash -u ${UID} -g ${USER} ${USER}
USER ${USER}

RUN curl https://sh.rustup.rs -sSf >> ${HOME}/rustup.rs
RUN sh ${HOME}/rustup.rs -y
RUN echo $HOME
ENV PATH=/home/${USER}/.cargo/bin:$PATH
RUN echo $PATH

RUN rustup install nightly

# https://www.reddit.com/r/rust/comments/iingya/compiler_regression_on_1460/
RUN rustup default 1.45.2