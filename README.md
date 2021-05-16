# CrabeDB - A simple, yet mighty, KV store written in Rust.

![Crab](doc/crab.jpeg)
The **Norwegian Monster Crab** for you dear rustaceans !

## Option 1 : Build with Docker

For the client :

* `docker build -t crabedb-client:alpha -f docker/client.Dockerfile .`

* and then :

* `docker run crabedb-client:alpha -e params="<CLIENT_CLI_PARAMS>"`


For the server :

* `docker build -t crabedb-server:alpha -f docker/server.Dockerfile .`

* and then :

* `docker run crabedb-server:alpha -e params="<SERVER_CLI_PARAMS>"`

## Option 2 : Build from Cargo

You should have the Rust toolchain installed on your machine with Cargo.

Build with `cargo build --release`

Then the binaries are in `/target/release`

The binaries for the client and server are respectively `crabedb-client` and `crabedb-server` (followed by the extension `.exe` if you are on windows)
