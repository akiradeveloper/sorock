FROM rust:latest

# needed for lol-core
RUN rustup component add rustfmt

# given the existing project structure, from lol-admin dir
# you run from one dir up to capture lol-core dependency
# docker build -t lol-admin -f Dockerfile ..

# then we have access to whole project for lol-core dependency
COPY ./ /lol
# TODO: could parameterize this so it could target lol-monitor
# though here we only want to build admin
WORKDIR /lol/lol-admin

RUN cargo build --release

# the build goes back into the parent lol dir
# when running, bind to the host so we can hit localhost too
# docker run --network="host" lol-admin
ENTRYPOINT ["../target/release/lol-admin"]
