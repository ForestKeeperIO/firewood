use clap::{command, Arg};
use firewood::db::{DBConfig, DB};

fn main() {
    let matches = command!()
        .arg(Arg::new("INPUT").help("db path name").required(true).index(1))
        .get_matches();
    let cfg = DBConfig::builder()
        .meta_ncached_pages(1024)
        .meta_ncached_files(128)
        .compact_ncached_pages(1024)
        .compact_ncached_files(128);
    let db = DB::new(
        matches.get_one::<String>("INPUT").unwrap(),
        &cfg.truncate(false).build(),
    )
    .unwrap();
    println!("== Account Model ==\n{}", db.dump());
    println!("== Generic KV ==\n{}", db.kv_dump());
}
