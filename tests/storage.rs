use firewood::db::{DBConfig, DB};

#[test]
fn test_persistent_merkle_simple() {
    let cfg = DBConfig::builder()
        .meta_ncached_pages(1024)
        .meta_ncached_files(128)
        .compact_ncached_pages(1024)
        .compact_ncached_files(128);
    {
        let db = DB::new("persistent_merkle_simple", &cfg.clone().truncate(true).build()).unwrap();
        let items = vec![
            ("do", "verb"),
            ("doe", "reindeer"),
            ("dog", "puppy"),
            ("doge", "coin"),
            ("horse", "stallion"),
            ("ddd", "ok"),
        ];
        let mut wb = db.new_writebatch();
        for (k, v) in items.iter() {
            wb.insert(k, v.as_bytes().to_vec()).unwrap();
        }
        wb.commit();
        println!("{}\n{}", hex::encode(&*db.root_hash()), db.dump());
    }
    {
        let db = DB::new("persistent_merkle_simple", &cfg.truncate(false).build()).unwrap();
        println!("{}\n{}", hex::encode(&*db.root_hash()), db.dump());
        let mut wb = db.new_writebatch();
        wb.insert(b"dough", b"sweet".to_vec()).unwrap();
        wb.commit();
        println!("{}\n{}", hex::encode(&*db.root_hash()), db.dump());
    }
}
