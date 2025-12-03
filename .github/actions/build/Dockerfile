FROM rust:latest
RUN cargo install mdbook-mermaid
RUN cargo install mdbook --no-default-features --features search --vers "^0.4" --locked
CMD ["mdbook", "build", "book"]