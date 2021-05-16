# CrabeDB - A simple, yet mighty, KV store written in Rust.

![Crab](doc/crab.jpeg)
The **Norwegian Monster Crab** for you dear rustaceans !

## Option 1 : Build with Docker

### Build

**For the client :**

* You can build it yourself :
    * `docker build -t crabedb-client:alpha -f docker/client.Dockerfile .`

* Or pull it from this repository :
    * `docker pull gabrielmougard/crabedb-client:alpha`


**For the server :**

* You can build it yourself :

    * `docker build -t crabedb-server:alpha -f docker/server.Dockerfile .`

* Or pull it from this repository :

    * `docker pull gabrielmougard/crabedb-server:alpha`


### Networking

Create a docker network:

* `sudo docker network create crabedb_network`

### Run

**Run the client :**

```bash
docker run\
--network crabedb_network\
crabedb-client:alpha\
-e params="<CLIENT_CLI_PARAMS>"
```

(if you don't enter any params, flag `-h` is chosen by default so you can have an overview of the commands.)

NOTE : If you run this code in the above docker network, the default node 127.0.0.1:5000 should work. Else, you will have to export the DOCKERHOST as environment variable in the dockerfile...

**Run the server :**

```bash
docker run\
--network crabedb_network\
crabedb-server:alpha\
-e params="<CLIENT_SERVER_PARAMS>"
```

(if you don't enter any params, flag `-h` is chosen by default so you can have an overview of the commands.)

## Option 2 : Build from Cargo

You should have the Rust toolchain installed on your machine with Cargo.

Build with `cargo build --release`

Then the binaries are in `/target/release`

The binaries for the client and server are respectively `crabedb-client` and `crabedb-server` (followed by the extension `.exe` if you are on windows)

NOTE : At least here, you shouldn't have any networking complexities when running the binaries...