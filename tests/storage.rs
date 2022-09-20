use firewood::db::{MerkleDB, MerkleDBConfig};

#[test]
fn test_persistent_merkle_simple() {
    let db = MerkleDB::new(
        "persistent_merkle_simple",
        &MerkleDBConfig::builder()
            .meta_ncached_pages(1024)
            .meta_ncached_files(128)
            .compact_ncached_pages(1024)
            .compact_ncached_files(128)
            .truncate(true)
            .build(),
    );
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
}
