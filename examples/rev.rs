use firewood::db::{DBConfig, WALConfig, DB};

fn main() {
    let cfg = DBConfig::builder()
        .meta_ncached_pages(1024)
        .meta_ncached_files(128)
        .compact_ncached_pages(1024)
        .compact_ncached_files(128)
        .wal(WALConfig::builder().max_revisions(10).build());
    {
        let db = DB::new("rev_db", &cfg.clone().truncate(true).build()).unwrap();
        let items = vec![
            ("do", "verb"),
            ("doe", "reindeer"),
            ("dog", "puppy"),
            ("doge", "coin"),
            ("horse", "stallion"),
            ("ddd", "ok"),
        ];
        for (k, v) in items.iter() {
            let mut wb = db.new_writebatch();
            wb.kv_insert(k, v.as_bytes().to_vec()).unwrap();
            println!("{}", hex::encode(*wb.kv_root_hash().unwrap()));
            wb.commit();
        }
        db.kv_dump(&mut std::io::stdout()).unwrap();
        println!(
            "{}",
            hex::encode(*db.get_revision(1, None).unwrap().kv_root_hash().unwrap())
        );
        println!(
            "{}",
            hex::encode(*db.get_revision(2, None).unwrap().kv_root_hash().unwrap())
        );
    }
    {
        let db = DB::new("rev_db", &cfg.truncate(false).build()).unwrap();
        {
            let rev = db.get_revision(1, None).unwrap();
            print!("{}", hex::encode(*rev.kv_root_hash().unwrap()));
            rev.kv_dump(&mut std::io::stdout()).unwrap();
        }
        {
            let rev = db.get_revision(2, None).unwrap();
            print!("{}", hex::encode(*rev.kv_root_hash().unwrap()));
            rev.kv_dump(&mut std::io::stdout()).unwrap();
        }
    }
}
