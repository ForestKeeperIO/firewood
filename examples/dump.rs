use clap::{command, Arg, ArgMatches};
use firewood::db::{DBConfig, DBError, WALConfig, DB};

fn main() {
    let matches = command!()
        .arg(Arg::new("INPUT").help("db path name").required(false).index(1))
        .get_matches();
    let cfg = DBConfig::builder()
        .meta_ncached_pages(1024)
        .meta_ncached_files(128)
        .compact_ncached_pages(1024)
        .compact_ncached_files(128);
    let path = get_db_path(matches);
    let db = DB::new(path.unwrap().as_str(), &cfg.truncate(false).build()).unwrap();
    let mut stdout = std::io::stdout();
    println!("== Account Model ==");
    db.dump(&mut stdout).unwrap();
    println!("== Generic KV ==");
    db.kv_dump(&mut stdout).unwrap();
}

/// Returns the provided INPUT db path if one is provided.
/// Otherwise, instantiate a DB called simple_db and return the path.
fn get_db_path(matches: ArgMatches) -> Result<String, DBError> {
    if let Some(m) = matches.get_one::<String>("INPUT") {
        return Ok(m.to_string())
    }

    // Build and provide a new db path
    let cfg = DBConfig::builder()
        .meta_ncached_pages(1024)
        .meta_ncached_files(128)
        .compact_ncached_pages(1024)
        .compact_ncached_files(128)
        .wal(WALConfig::builder().max_revisions(10).build());
    let db = DB::new("simple_db", &cfg.truncate(true).build()).unwrap();
    {
        let mut wb = db.new_writebatch();
        wb.set_balance(b"ted", 10.into()).unwrap();
        wb.set_code(b"ted", b"smart contract byte code here!").unwrap();
        wb.set_nonce(b"ted", 10086).unwrap();
        wb.set_state(b"ted", b"x", b"1".to_vec()).unwrap();
        wb.set_state(b"ted", b"y", b"2".to_vec()).unwrap();
        wb.commit();
    }
    Ok("simple_db".to_string())
}
