FROM rust:1.78-bookworm AS chef
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
RUN RUSTFLAGS="--cfg tokio_unstable" cargo build --release

FROM rust:1.78-slim-bookworm AS runtime
EXPOSE 50000
EXPOSE 6669
COPY --from=builder /work/target/release/testapp ./
ENTRYPOINT ["./testapp"]