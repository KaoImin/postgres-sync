use ckb_indexer::service::{gen_client::Client, get_block_by_number};
use clap::{crate_version, App, Arg};
use jsonrpc_core_client::transports::http;
use log4rs::config::{Appender, Root};
use log4rs::{append::file::FileAppender, Config};
use xsql::{XSQLPool, DB};

use std::time::Instant;

#[tokio::main]
async fn main() {
    log_init();
    let matches = App::new("mercury")
        .version(crate_version!())
        .arg(
            Arg::with_name("host")
                .short("h")
                .long("host")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("port")
                .short("p")
                .long("port")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("user")
                .short("u")
                .long("user")
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

    log::info!("start");

    let ckb_client: Client = http::connect("http://127.0.0.1:8114").await.unwrap();
    let db = XSQLPool::new_pgsql(
        "mercury",
        matches.value_of("host").unwrap_or("127.0.0.1"),
        matches
            .value_of("port")
            .unwrap_or("8432")
            .parse::<u16>()
            .unwrap(),
        matches.value_of("user").unwrap_or("postgres"),
        matches.value_of("password").unwrap(),
        100,
        1,
        1,
    )
    .await;

    let mut now = Instant::now();
    for num in 0..2_000_000u64 {
        let block = get_block_by_number(&ckb_client, num, true)
            .await
            .unwrap()
            .unwrap();

        log::info!("append {} block", num);
        if let Err(e) = db.append_block(block.clone()).await {
            log::error!("append {} error {:?}", num, e);
            log::error!("block {:?}", block);
            return;
        }

        if num % 50000 == 0 {
            log::info!("append 50000 block cost {:?}", Instant::now() - now);
            now = Instant::now();
        }
    }
}

fn log_init() {
    let root = Root::builder()
        .appender("file")
        .build(log::LevelFilter::Info);
    let file_appender = FileAppender::builder().build("./log/log.log").unwrap();
    let config = Config::builder()
        .appender(Appender::builder().build("file", Box::new(file_appender)))
        .build(root)
        .unwrap();
    log4rs::init_config(config).unwrap();
}
