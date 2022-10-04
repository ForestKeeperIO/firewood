use std::collections::VecDeque;

#[test]
fn test_revisions() {
    use firewood::db::{DBConfig, DiskBufferConfig, DB};
    use rand::{rngs::StdRng, seq::SliceRandom, Rng, SeedableRng};
    let cfg = DBConfig::builder()
        .meta_ncached_pages(1024)
        .meta_ncached_files(128)
        .compact_ncached_pages(1024)
        .compact_ncached_files(128)
        .compact_file_nbit(16)
        .compact_regn_nbit(16)
        .buffer(
            DiskBufferConfig::builder()
                .wal_file_nbit(15)
                .wal_block_nbit(8)
                .max_revisions(10)
                .build(),
        );

    let rng = std::cell::RefCell::new(StdRng::seed_from_u64(42));
    let max_len0 = 8;
    let max_len1 = 4;
    let keygen = || {
        let (len0, len1): (usize, usize) = {
            let mut rng = rng.borrow_mut();
            (rng.gen_range(1..max_len0 + 1), rng.gen_range(1..max_len1 + 1))
        };
        let key: Vec<u8> = (0..len0)
            .map(|_| rng.borrow_mut().gen_range(0..2))
            .chain((0..len1).map(|_| rng.borrow_mut().gen()))
            .collect();
        key
    };
    for i in 0..10 {
        let db = DB::new("test_revisions_db", &cfg.clone().truncate(true).build()).unwrap();
        let mut dumped = VecDeque::new();
        for _ in 0..100 {
            {
                let mut wb = db.new_writebatch();
                let m = rng.borrow_mut().gen_range(1..20);
                for _ in 0..m {
                    let key = keygen();
                    let val: Vec<u8> = (0..8).map(|_| rng.borrow_mut().gen()).collect();
                    wb.insert(key, val.to_vec()).unwrap();
                }
                wb.commit();
            }
            while dumped.len() > 10 {
                dumped.pop_back();
            }
            dumped.push_front(db.dump());
            for i in 1..dumped.len() {
                let rev = db.get_revision(i, None).unwrap();
                assert_eq!(rev.dump(), dumped[i]);
            }
        }
        drop(db);
        let db = DB::new("test_revisions_db", &cfg.clone().truncate(false).build()).unwrap();
        for i in 1..dumped.len() {
            let rev = db.get_revision(i, None).unwrap();
            assert_eq!(rev.dump(), dumped[i]);
        }
        println!("i = {}", i);
    }
}
