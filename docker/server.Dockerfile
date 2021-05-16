FROM rust:latest
RUN rustup component add rustfmt
RUN USER=root cargo new --bin crabedb-server
WORKDIR /crabedb-server
COPY ./Cargo.toml ./Cargo.toml
COPY build.rs build.rs
COPY proto/ proto/
COPY src/ src/

RUN cargo build
ENV RUST_LOG info

EXPOSE 5000
ENTRYPOINT ["/bin/bash", "-c"]
CMD ["./target/debug/crabedb-server -h"]