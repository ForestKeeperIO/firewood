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
        //println!("{}\n{}", hex::encode(&*db.root_hash()), db.dump());
    }
    {
        let db = DB::new("simple_db", &cfg.clone().truncate(false).build()).unwrap();
        //println!("{}\n{}", hex::encode(&*db.root_hash()), db.dump());
        let mut wb = db.new_writebatch();
        wb.insert(b"dough", b"sweet".to_vec()).unwrap();
        wb.commit();
        //println!("{}\n{}", hex::encode(&*db.root_hash()), db.dump());
    }
    {
        let db = DB::new("simple_db", &cfg.clone().truncate(true).build()).unwrap();
        println!("{}\n{}", hex::encode(&*db.root_hash()), db.dump());
        use rand::{Rng, SeedableRng};
        let mut rng = rand::rngs::StdRng::seed_from_u64(0);
        let mut workload = Vec::new();
        for _ in 0..30000 {
            let mut wb = Vec::new();
            for _ in 0..100 {
                let key = rng.gen::<[u8; 32]>();
                let val = "b".repeat(rng.gen_range(1..10));
                wb.push((key, val))
            }
            workload.push(wb);
        }
        println!("workload prepared");

        for w in workload {
            let mut wb = db.new_writebatch();
            for (key, val) in w {
                wb.insert(key, val.into()).unwrap();
            }
            wb.commit();
            //println!("commit");
        }
        //println!("{}\n{}", hex::encode(&*db.root_hash()), db.dump());
    }
    println!("here");
    {
        //let db = DB::new("simple_db", &cfg.truncate(false).build()).unwrap();
        //println!("{}\n{}", hex::encode(&*db.root_hash()), db.dump());
    }
}
