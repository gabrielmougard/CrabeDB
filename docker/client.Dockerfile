FROM rust:latest as cargo-build
RUN apt-get update
RUN apt-get install musl-tools -y
RUN rustup target add x86_64-unknown-linux-musl
RUN rustup component add rustfmt
WORKDIR /usr/src/crabedb-client

COPY Cargo.toml Cargo.toml
COPY build.rs build.rs
COPY proto/ proto/
COPY src/ src/

RUN RUSTFLAGS=-Clinker=musl-gcc cargo build --release --target=x86_64-unknown-linux-musl
RUN rm -f target/x86_64-unknown-linux-musl/release/deps/crabedb-client*

FROM alpine:latest
RUN addgroup -g 1000 crabedb-client
RUN adduser -D -s /bin/sh -u 1000 -G crabedb-client crabedb-client
WORKDIR /home/crabedb-client/bin/
COPY --from=cargo-build /usr/src/crabedb-client/target/x86_64-unknown-linux-musl/release/crabedb-client .
RUN chown crabedb-client:crabedb-client crabedb-client
USER crabedb-client

ENV RUST_LOG debug
ENV PARAMS -h

CMD ["sh", "-c", "crabedb-client ${PARAMS}"]