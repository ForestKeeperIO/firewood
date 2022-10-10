use firewood::db::{DBConfig, WALConfig, DB};

fn main() {
    let cfg = DBConfig::builder()
        .meta_ncached_pages(1024)
        .meta_ncached_files(128)
        .compact_ncached_pages(1024)
        .compact_ncached_files(128)
        .wal(WALConfig::builder().max_revisions(10).build());
    {
        let db = DB::new("simple_db", &cfg.clone().truncate(true).build()).unwrap();
        {
            let mut wb = db.new_writebatch();
            wb.set_balance(b"ted", 10.into()).unwrap();
            wb.set_code(b"ted", b"smart contract byte code here!").unwrap();
            wb.set_nonce(b"ted", 10086).unwrap();
            wb.commit();
        }
        println!("{}\n{}", hex::encode(&*db.root_hash()), db.dump());
    }
    {
        let db = DB::new("simple_db", &cfg.clone().truncate(false).build()).unwrap();
        for account in ["ted", "alice"] {
            let addr = account.as_bytes();
            println!("{}.balance = {}", account, db.get_balance(addr).unwrap());
            println!("{}.nonce = {}", account, db.get_nonce(addr).unwrap());
            println!(
                "{}.code = {}",
                account,
                std::str::from_utf8(&db.get_code(addr).unwrap()).unwrap()
            );
        }
    }
}
