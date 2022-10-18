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
    let mut stdout = std::io::stdout();
    println!("== Account Model ==");
    db.dump(&mut stdout).unwrap();
    println!("== Generic KV ==");
    db.kv_dump(&mut stdout).unwrap();
}
