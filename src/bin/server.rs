use std::str;
use std::convert::From;

use log::{info, debug};
use tonic::transport::Server;
use tonic::{Request, Response, Status};
use clap::{Arg, App};
pub mod protobuf {
    tonic::include_proto!("kvstore");
}
use protobuf::kvstore_server::{Kvstore, KvstoreServer};
use protobuf::{
    GetRequest, GetResponse,
    SetRequest, SetResponse,
    RemoveRequest, RemoveResponse
};

extern crate crabedb;
use crabedb::storage::crabe_db::CrabeDB;
use crabedb::storage::options::{StorageOptions, SyncOptions};

pub struct KvStoreAPI {
    db: CrabeDB,
    //telemetry: Option<Telemetry>,
}

#[tonic::async_trait]
impl Kvstore for KvStoreAPI {
    async fn kv_get_call(
        &self,
        request: Request<GetRequest>
    ) -> Result<Response<GetResponse>, Status> {
        let payload = request.into_inner();
        debug!("Key in payload: {:?}", &payload.key);

        let v = self.db.get(&payload.key)?;
        match v {
            Some(val) => {
                let response = GetResponse {
                    exist: true,
                    value: String::from(str::from_utf8(&val).unwrap()),
                };
                Ok(Response::new(response))
            }
            None => {
                let response = GetResponse {
                    exist: false,
                    value: String::from(""),
                };
                Ok(Response::new(response))
            }
        }
    }

    async fn kv_set_call(
        &self,
        request: Request<SetRequest>
    ) -> Result<Response<SetResponse>, Status> {
        let payload = request.into_inner();
        debug!("Key in payload: {:?}, Value in payload : {:?}", &payload.key, &payload.value);

        match self.db.set(&*payload.key, &*payload.value) {
            Ok(_) => {
                let response = SetResponse {
                    success: true,
                };
                Ok(Response::new(response))
            }
            Err(_) => {
                let response = SetResponse {
                    success: false,
                };
                Ok(Response::new(response))
            }
        }
    }

    async fn kv_remove_call(
        &self,
        request: Request<RemoveRequest>
    ) -> Result<Response<RemoveResponse>, Status> {
        let payload = request.into_inner();
        debug!("Key in payload: {:?}", &payload.key);

        match self.db.remove(&payload.key) {
            Ok(_) => {
                let response = RemoveResponse {
                    success: true,
                };
                Ok(Response::new(response))
            }
            Err(_) => {
                let response = RemoveResponse {
                    success: false,
                };
                Ok(Response::new(response))
            }
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let matches = App::new("
    .d8888b.                  888               8888888b.  888888b.
    d88P  Y88b                 888               888  'Y88b 888  '88b
    888    888                 888               888    888 888  .88P
    888        888d888 8888b.  88888b.   .d88b.  888    888 8888888K.
    888        888P'      '88b 888 '88b d8P  Y8b 888    888 888  'Y88b
    888    888 888    .d888888 888  888 88888888 888    888 888    888
    Y88b  d88P 888    888  888 888 d88P Y8b.     888  .d88P 888   d88P
     'Y8888P'  888    'Y888888 88888P'   'Y8888  8888888P'  8888888P'
    \n\n
    ")
    .version("0.1.0")
    .author("Gabriel Mougard <gabriel.mougard@gmail.com>")
    .about("gRPC server for the CrabeDB store")
    .arg(Arg::with_name("port")
        .short("p")
        .long("port")
        .help("Port number of the running server (default :5000)")
    )
    .arg(Arg::with_name("dumps")
        .short("d")
        .long("dump")
        .help("Path of a dump file for memory recovery and data persistence.")
    )
    .get_matches();

    let port = matches.value_of("port").unwrap_or("5000");
    let addr = format!("[::1]:{}", port).parse().unwrap();
    let dump_path = match matches.value_of("dump") {
        Some(path) => path,
        None => "",
    };
    //let kv_store_api = KvStoreAPI::default();
    let db = StorageOptions::default()
        .compaction_check_frequency(1200)
        .sync(SyncOptions::Frequency(5000))
        .max_file_size(1024 * 1024 * 1024)
        .open("test.db")?;

    let kv_store_api = KvStoreAPI { db };
    info!("CrabeDB Server listening on {}", addr);

    Server::builder()
        .add_service(KvstoreServer::new(kv_store_api))
        .serve(addr)
        .await?;

    Ok(())
}