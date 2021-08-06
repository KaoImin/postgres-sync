use ckb_indexer::service::{gen_client::Client, get_block_by_number};
use clap::{crate_version, App, Arg};
use jsonrpc_core_client::transports::http;
use xsql::{XSQLPool, DB};

use std::time::Instant;

#[tokio::main]
async fn main() {
    let matches = App::new("mercury")
        .version(crate_version!())
        .arg(
            Arg::with_name("host")
                .short("h")
                .long("host")
                .required(true)
                .takes_value(true),
        )
        .arg(
            Arg::with_name("port")
                .short("p")
                .long("port")
                .required(true)
                .takes_value(true),
        )
        .arg(
            Arg::with_name("user")
                .short("u")
                .long("user")
                .required(true)
                .takes_value(true),
        )
        .arg(
            Arg::with_name("password")
                .short("w")
                .long("pwd")
                .required(true)
                .takes_value(true),
        )
        .get_matches();

    let ckb_client: Client = http::connect("http://127.0.0.1:8114").await.unwrap();
    let db = XSQLPool::new(
        matches.value_of("host").unwrap(),
        matches.value_of("port").unwrap().parse::<u16>().unwrap(),
        matches.value_of("user").unwrap(),
        matches.value_of("password").unwrap(),
        100,
    )
    .await;

    let mut now = Instant::now();
    for num in 0..2_000_000u64 {
        let block = get_block_by_number(&ckb_client, num, true)
            .await
            .unwrap()
            .unwrap();

        log::info!("append {} block", num);
        if let Err(e) = db.append_block(block).await {
            log::error!("append {} error {:?}", num, e);
        }

        if num % 50000 == 0 {
            log::info!("append 50000 block cost {:?}", Instant::now() - now);
            now = Instant::now();
        }
    }
}
