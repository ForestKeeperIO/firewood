use firewood::db::{DBConfig, DiskBufferConfig, DB};

fn main() {
    let cfg = DBConfig::builder()
        .meta_ncached_pages(1024)
        .meta_ncached_files(128)
        .compact_ncached_pages(1024)
        .compact_ncached_files(128)
        .buffer(
            DiskBufferConfig::builder()
                .wal_file_nbit(9)
                .wal_block_nbit(8)
                .max_revisions(0)
                .build(),
        );
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
        let db = DB::new("persistent_merkle_simple", &cfg.clone().truncate(false).build()).unwrap();
        println!("{}\n{}", hex::encode(&*db.root_hash()), db.dump());
        let mut wb = db.new_writebatch();
        wb.insert(b"dough", b"sweet".to_vec()).unwrap();
        wb.commit();
        println!("{}\n{}", hex::encode(&*db.root_hash()), db.dump());
    }
    {
        let db = DB::new("persistent_merkle_simple", &cfg.truncate(false).build()).unwrap();
        println!("{}\n{}", hex::encode(&*db.root_hash()), db.dump());
        use rand::{Rng, SeedableRng};
        let mut rng = rand::rngs::StdRng::seed_from_u64(0);
        for _ in 0..10 {
            let mut wb = db.new_writebatch();
            for _ in 0..10 {
                let key = "a".repeat(rng.gen_range(1..100));
                let val = "b".repeat(rng.gen_range(1..32));
                wb.insert(key.as_bytes(), val.into()).unwrap();
            }
            wb.commit();
        }
        println!("{}\n{}", hex::encode(&*db.root_hash()), db.dump());
    }
}
