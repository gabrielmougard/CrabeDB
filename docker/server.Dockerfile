FROM rust:latest as cargo-build
RUN apt-get update
RUN apt-get install musl-tools -y
RUN rustup target add x86_64-unknown-linux-musl
RUN rustup component add rustfmt
WORKDIR /usr/src/crabedb-server

COPY Cargo.toml Cargo.toml
COPY build.rs build.rs
COPY proto/ proto/
COPY src/ src/

RUN RUSTFLAGS=-Clinker=musl-gcc cargo build --release --target=x86_64-unknown-linux-musl
RUN rm -f target/x86_64-unknown-linux-musl/release/deps/crabedb-server*

FROM alpine:latest
RUN addgroup -g 1000 crabedb-server
RUN adduser -D -s /bin/sh -u 1000 -G crabedb-server crabedb-server
WORKDIR /home/crabedb-server/bin/
COPY --from=cargo-build /usr/src/crabedb-server/target/x86_64-unknown-linux-musl/release/crabedb-server .
RUN chown crabedb-server:crabedb-server crabedb-server
USER crabedb-server

ENV RUST_LOG debug
ENV params -h
EXPOSE 5000

CMD ["sh", "-c", "crabedb-server ${params}"]