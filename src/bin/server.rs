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
use regex::Regex;

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
    env_logger::init();
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
        .help("Port number of the running server. (default: 5000)")
        .takes_value(true)
    )
    .arg(Arg::with_name("dump")
        .short("d")
        .long("dump")
        .help("Path of a dump file for memory recovery and data persistence. (default: crabe.db)")
        .takes_value(true)
    )
    .arg(Arg::with_name("sync-frequency")
        .long("sync-frequency")
        .help("In milliseconds, it describes the frequency of the synchronisation process the in-mem data and the dump. (default: 2000)")
        .takes_value(true)
    )
    .arg(Arg::with_name("max-file-size")
        .long("max-file-size")
        .help("Set the max file size, in bytes, for a dump. Then, another dump will be created. (default: 1073741824) => 1GB")
        .takes_value(true)
    )
    .arg(Arg::with_name("enable-compaction")
        .long("enable-compaction")
        .help("Enable the compaction of the dumps. (default: true)")
        .takes_value(true)
    )
    .arg(Arg::with_name("compaction-frequency")
        .long("compaction-frequency")
        .help("The frequency of compaction, in seconds. (default: 3600)")
        .takes_value(true)
    )
    .arg(Arg::with_name("compaction-window")
        .long("compaction-window")
        .help("The time window (<start_hour>:<end_hour>) during which compaction can run. (default: 0:23)")
        .takes_value(true)
    )
    .arg(Arg::with_name("descriptor-cache-size")
        .long("descriptor-cache-size")
        .help("Maximum size, in bytes, of the file descriptor cache. (default: 2048)")
        .takes_value(true)
    )
    .arg(Arg::with_name("fragmentation-trigger")
        .long("fragmentation-trigger")
        .help("The ratio of dead entries to total entries in a file that will trigger compaction. (default: 0.6)")
        .takes_value(true)
    )
    .arg(Arg::with_name("fragmentation-threshold")
        .long("fragmentation-threshold")
        .help("The ratio of dead entries to total entries in a file that will cause it to be included in a compaction. (default: 0.4)")
        .takes_value(true)
    )
    .arg(Arg::with_name("dead-bytes-trigger")
        .long("dead-bytes-trigger")
        .help("The minimum amount of data occupied by dead entries in a single file that will trigger compaction, in bytes. (default: 536870912) => 512MB")
        .takes_value(true)
    )
    .arg(Arg::with_name("dead-bytes-threshold")
        .long("dead-bytes-threshold")
        .help("The minimum amount of data occupied by dead entries in a single file that will cause it to be included in a compaction. (default: 134217728) => 128MB")
        .takes_value(true)
    )
    .arg(Arg::with_name("small-file-threshold")
        .long("small-file-threshold")
        .help("the minimum size a file must have to be excluded from compaction. (default: 10485760) => 10MB")
        .takes_value(true)
    )
    .get_matches();

    let port = match matches.value_of("port") {
        Some(p) => {
            match p.parse::<u32>() {
                Ok(arg) => {
                    if arg <= 65535 {
                        arg
                    } else {
                        5000
                    }
                },
                Err(_) => 5000,
            }
        },
        None => 5000,
    };
    let dump_path = match matches.value_of("dump") {
        Some(path) => path,
        None => "crabe.db",
    };
    let sync_freq = match matches.value_of("sync-frequency") {
        Some(sf) => {
            match sf.parse::<usize>() {
                Ok(arg) => arg,
                Err(_) => 2000,
            }
        },
        None => 2000,
    };
    let max_file_size = match matches.value_of("max-file-size") {
        Some(mfs) => {
            match mfs.parse::<usize>() {
                Ok(arg) => arg,
                Err(_) => 1073741824,
            }
        },
        None => 1073741824,
    };
    let enable_compaction = match matches.value_of("enable-compaction") {
        Some(ec) => {
            match ec.parse::<bool>() {
                Ok(arg) => arg,
                Err(_) => true,
            }
        },
        None => true,
    };
    let compaction_frequency = match matches.value_of("compaction-frequency") {
        Some(cf) => {
            match cf.parse::<u64>() {
                Ok(arg) => arg,
                Err(_) => 3600,
            }
        },
        None => 3600,
    };
    let (start_compaction, end_compaction) = match matches.value_of("compaction-window") {
        Some(cw) => {
            let re = Regex::new(r"([0-9]{1,2}):([0-9]{1,2})").unwrap();
            match re.captures(cw) {
                Some(cap) => {
                    let start_win = cap[1].parse::<usize>().unwrap();
                    let end_win = cap[2].parse::<usize>().unwrap();

                    if start_win <= 23 && end_win <= 23 && start_win < end_win {
                        (start_win, end_win)
                    } else {
                        (0,23)
                    }
                },
                None => (0,23),
            }
        },
        None => (0, 23),
    };
    let descriptor_cache_size = match matches.value_of("descriptor-cache-size") {
        Some(dcs) => {
            match dcs.parse::<usize>() {
                Ok(arg) => arg,
                Err(_) => 2048,
            }
        },
        None => 2048,
    };
    let fragmentation_trigger = match matches.value_of("fragmentation-trigger") {
        Some(ftrig) => {
            match ftrig.parse::<f64>() {
                Ok(arg) => arg,
                Err(_) => 0.6,
            }
        },
        None => 0.6,
    };
    let fragmentation_threshold = match matches.value_of("fragmentation-threshold") {
        Some(fthres) => {
            match fthres.parse::<f64>() {
                Ok(arg) => arg,
                Err(_) => 0.4,
            }
        },
        None => 0.4,
    };
    let dead_bytes_trigger = match matches.value_of("dead-bytes-trigger") {
        Some(dbytestrig) => {
            match dbytestrig.parse::<u64>() {
                Ok(arg) => arg,
                Err(_) => 536870912,
            }
        },
        None => 536870912,
    };
    let dead_bytes_threshold = match matches.value_of("dead-bytes-threshold") {
        Some(dbytesthres) =>
            match dbytesthres.parse::<u64>() {
                Ok(arg) => arg,
                Err(_) => 134217728,
            },
        None => 134217728
    };
    let small_file_threshold = match matches.value_of("small-file-threshold") {
        Some(sft) => {
            match sft.parse::<u64>() {
                Ok(arg) => arg,
                Err(_) => 10485760,
            }
        },
        None => 10485760,
    };

    let db = StorageOptions::default()
        .sync(SyncOptions::Frequency(sync_freq))
        .max_file_size(max_file_size)
        .file_chunk_queue_size(descriptor_cache_size)
        .compaction(enable_compaction)
        .compaction_check_frequency(compaction_frequency)
        .compaction_window(start_compaction, end_compaction)
        .fragmentation_trigger(fragmentation_trigger)
        .fragmentation_threshold(fragmentation_threshold)
        .dead_bytes_trigger(dead_bytes_trigger)
        .dead_bytes_threshold(dead_bytes_threshold)
        .small_file_threshold(small_file_threshold)
        .open(dump_path)?;

    let kv_store_api = KvStoreAPI { db };
    let addr = format!("[::1]:{}", port).parse().unwrap();
    info!("CrabeDB Server listening on {}", addr);

    Server::builder()
        .add_service(KvstoreServer::new(kv_store_api))
        .serve(addr)
        .await?;

    Ok(())
}