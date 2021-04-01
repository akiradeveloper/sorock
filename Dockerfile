FROM 'centos:7'
WORKDIR '/lol-root'

RUN yum install -y sudo gcc iputils bind-utils make
RUN yum install -y ruby
RUN yum install -y clang gcc-c++

ARG USER
ARG UID
RUN groupadd ${USER}
RUN useradd -d /home/${USER} -m -s /bin/bash -u ${UID} -g ${USER} -G wheel,root ${USER}
RUN echo '%wheel ALL=(ALL) NOPASSWD:ALL' >> /etc/sudoers
USER ${USER}

RUN curl https://sh.rustup.rs -sSf >> ${HOME}/rustup.rs
RUN sh ${HOME}/rustup.rs -y
RUN echo $HOME
ENV PATH=/home/${USER}/.cargo/bin:$PATH
RUN echo $PATH

RUN rustup install 1.51.0
RUN rustup install nightly
RUN rustup default 1.51.0