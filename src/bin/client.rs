use log::{info, warn};
use clap::{Arg, App, SubCommand};
use protobuf::{GetRequest, SetRequest, RemoveRequest};
use protobuf::kvstore_client::KvstoreClient;
pub mod protobuf {
    tonic::include_proto!("kvstore");
}
use regex::Regex;

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
    .about("gRPC client for the CrabeDB store")
    .arg(Arg::with_name("node")
        .help("IP address of the target CrabeDB server (default: 127.0.0.1:5000)")
        .required(true)
        .index(1)
    )
    .subcommand(
        SubCommand::with_name("get")
            .about("Get a the value of the given key from the remote server.")
            .version("0.1.0")
            .author("Gabriel Mougard <gabriel.mougard@gmail.com>")
            .arg(Arg::with_name("key")
                .help("The key you want to get.")
                .required(true)
                .index(1)
            )
    )
    .subcommand(
        SubCommand::with_name("set")
            .about("Set a key/value in the remote server.")
            .version("0.1.0")
            .author("Gabriel Mougard <gabriel.mougard@gmail.com>")
            .arg(Arg::with_name("key")
                .help("The name of the key you want to set.")
                .required(true)
                .index(1)
            )
            .arg(Arg::with_name("value")
                .help("The value associated with the key you want to set.")
                .required(true)
                .index(2)
            )
    )
    .subcommand(
        SubCommand::with_name("remove")
            .about("Remove a key/value in the remote server.")
            .version("0.1.0")
            .author("Gabriel Mougard <gabriel.mougard@gmail.com>")
            .arg(Arg::with_name("key")
                .help("The name of the key you want to remove.")
                .required(true)
                .index(1)
            )
    )
    .get_matches();

    let node_addr = match matches.value_of("node") {
        Some(target) => {
            let re_ip = Regex::new(r"(([0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])\.){3}([0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5]):([0-9]{1,})$").unwrap();
            let re_valid_dns = Regex::new(r"(([a-zA-Z]|[a-zA-Z][a-zA-Z0-9\-]*[a-zA-Z0-9])\.)*([A-Za-z]|[A-Za-z][A-Za-z0-9\-]*[A-Za-z0-9])$").unwrap();
            match re_ip.captures(target) {
                Some(cap) => cap.get(1).unwrap().as_str(),
                None => {
                    match re_valid_dns.captures(target) {
                        Some(cap) => cap.get(1).unwrap().as_str(),
                        None => "127.0.0.1:5000",
                    }
                },
            }
        },
        None => "127.0.0.1:5000",
    };
    let mut tx = KvstoreClient::connect(format!("http://{}", node_addr)).await?;

    match matches.subcommand() {
        ("get", Some(get_subcommand)) => {
            match get_subcommand.value_of("key") {
                Some(key) => {
                    let request = tonic::Request::new(GetRequest {
                        key: String::from(key),
                    });
                    let response = tx.kv_get_call(request).await?;
                    if response.get_ref().exist {
                        info!("Retrieved value: {:?} for Key: {:?}", response.get_ref().value, key);
                    } else {
                        warn!("Key: {:?} doesn't exist.", key);
                    }
                },
                None => {}
            }
        },
        ("set", Some(set_subcommand)) => {
            match set_subcommand.value_of("key") {
                Some(key) => {
                    match set_subcommand.value_of("value") {
                        Some(value) => {
                            let request = tonic::Request::new(SetRequest {
                                key: String::from(key),
                                value: String::from(value),
                            });
                            let response = tx.kv_set_call(request).await?;
                            if response.get_ref().success {
                                info!("Key: {:?} has been successfully set with Value: {:?}", key, value);
                            } else {
                                warn!("Key: {:?} couldn't be set.", key);
                            }
                        },
                        None => {}
                    }
                },
                None => {}
            }
        },
        ("remove", Some(remove_subcommand)) => {
            match remove_subcommand.value_of("key") {
                Some(key) => {
                    let request = tonic::Request::new(RemoveRequest {
                        key: String::from(key),
                    });
                    let response = tx.kv_remove_call(request).await?;
                    if response.get_ref().success {
                        info!("Key: {:?} has been successfully removed with its value.", key);
                    } else {
                        warn!("Key: {:?} couldn't be removed.", key);
                    }
                },
                None => {}
            }
        },
        _ => {}
    }

    Ok(())
}