FROM rust:latest as cargo-build
RUN apt-get update
RUN apt-get install musl-tools -y
RUN rustup target add x86_64-unknown-linux-musl
WORKDIR /usr/src/crabedb-server

COPY Cargo.toml Cargo.toml
RUN mkdir src/
#RUN echo "fn main() {println!(\"if you see this, the build broke\")}" > src/main.rs
RUN RUSTFLAGS=-Clinker=musl-gcc cargo build --release --target=x86_64-unknown-linux-musl
RUN rm -f target/x86_64-unknown-linux-musl/release/deps/crabedb-server*

COPY build.rs build.rs
COPY proto/ .
COPY src/ .

RUN RUSTFLAGS=-Clinker=musl-gcc cargo build --release --target=x86_64-unknown-linux-musl

FROM alpine:latest
RUN addgroup -g 1000 crabedb-server
RUN adduser -D -s /bin/sh -u 1000 -G crabedb-server crabedb-server
WORKDIR /home/crabedb-server/bin/
COPY --from=cargo-build /usr/src/crabedb-server/target/x86_64-unknown-linux-musl/release/crabedb-server .
RUN chown crabedb-server:crabedb-server crabedb-server
USER crabedb-server

ENV params -h

# https://stackoverflow.com/questions/40873165/use-docker-run-command-to-pass-arguments-to-cmd-in-dockerfile
CMD ["sh", "-c", "crabedb-server ${params}"]