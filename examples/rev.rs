use firewood::db::{DBConfig, DiskBufferConfig, DB};

fn main() {
    let cfg = DBConfig::builder()
        .meta_ncached_pages(1024)
        .meta_ncached_files(128)
        .compact_ncached_pages(1024)
        .compact_ncached_files(128)
        .buffer(DiskBufferConfig::builder().max_revisions(10).build());
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
            wb.insert(k, v.as_bytes().to_vec()).unwrap();
            wb.commit();
            println!("{}", hex::encode(&*db.root_hash()));
        }
        println!("{}", db.dump());
        println!("{}", hex::encode(&*db.get_revision(1, None).unwrap().root_hash()));
        println!("{}", hex::encode(&*db.get_revision(2, None).unwrap().root_hash()));
    }
    {
        let db = DB::new("rev_db", &cfg.clone().truncate(false).build()).unwrap();
        {
            let rev = db.get_revision(1, None).unwrap();
            println!("{}\n{}", hex::encode(&*rev.root_hash()), rev.dump());
        }
        {
            let rev = db.get_revision(2, None).unwrap();
            println!("{}\n{}", hex::encode(&*rev.root_hash()), rev.dump());
        }
    }
}
