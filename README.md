# CrabeDB - A simple, yet mighty, KV store written in Rust.

![Crab](doc/crab.jpeg)
The **Norwegian Monster Crab** for you dear rustaceans !

## Option 1 : Build with Docker
(The docker images are not optimized in size, due to lack of time... But using a builder layer + alpine:latest should offer a size around 30MB for both images.)

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

First, **Run the server :**

```bash
docker run\
--network crabedb_network\
--name crabedbserver -p 5000:5000\
crabedb-server:alpha "./target/debug/crabedb-server <PARAMS>"
```

In order to work in the docker network, you should add the mandatory option `-a 0.0.0.0:5000`.
Of course, if you pull the image from the repository, replace `crabedb-server:alpha` by `gabrielmougard/crabedb-server:alpha`.

(enter `-h` so you can have an overview of the commands.)

**Run the client :**

You need to get the IP address of the server container over the network now.
Just type in `docker inspect <SERVER_CONTAINER_ID> | grep IPAddress`. I have `172.19.0.2` for example.
Then launch the client container with this IP address as the first <node> positionnal argument :

```bash
docker run\
--network crabedb_network\
crabedb-client:alpha "./target/debug/crabedb-client <PARAMS>"
```

Of course, if you pull the image from the repository, replace `crabedb-server:alpha` by `gabrielmougard/crabedb-server:alpha`.

(enter `-h` so you can have an overview of the commands.)


## Option 2 : Build from Cargo

You should have the Rust toolchain installed on your machine with Cargo.

Build with `cargo build --release`

Then the binaries are in `/target/release`

The binaries for the client and server are respectively `crabedb-client` and `crabedb-server` (followed by the extension `.exe` if you are on windows)

NOTE : At least here, you shouldn't have any networking complexities when running the binaries. The <node> argument in the client is 127.0.0.1:5000 and for the server binary, you don't have to enter the `-a` option, it will be 127.0.0.1:5000 by default.