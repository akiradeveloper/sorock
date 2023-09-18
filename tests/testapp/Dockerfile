FROM rust:1.72-bookworm as builder
WORKDIR /work
RUN apt-get update
RUN apt-get install -y protobuf-compiler

COPY . .
RUN cargo build

FROM rust:1.72-slim-bookworm as runtime
EXPOSE 50000
COPY --from=builder /work/target/debug/testapp ./
ENTRYPOINT ["./testapp"]