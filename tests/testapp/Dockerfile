FROM rust:1.75-bookworm AS chef
RUN apt-get update
RUN apt-get install -y protobuf-compiler
RUN cargo install cargo-chef
WORKDIR /work

FROM chef AS planner
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

FROM chef AS builder
COPY --from=planner /work/recipe.json recipe.json
RUN cargo chef cook --recipe-path recipe.json
COPY . .
RUN cargo build

FROM rust:1.75-slim-bookworm AS runtime
EXPOSE 50000
COPY --from=builder /work/target/debug/testapp ./
ENTRYPOINT ["./testapp"]