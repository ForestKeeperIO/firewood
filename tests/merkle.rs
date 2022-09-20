use std::rc::Rc;
use firewood::merkle::*;
use shale::{MemStore, MummyItem, ObjPtr, WriteContext};

fn merkle_setup_test(meta_size: u64, compact_size: u64) -> Merkle<shale::compact::CompactSpace> {
    use shale::{compact::CompactSpaceHeader, PlainMem};
    const RESERVED: u64 = 0x1000;
    assert!(meta_size > RESERVED);
    assert!(compact_size > RESERVED);
    let mem_meta = Rc::new(PlainMem::new(meta_size, 0x0)) as Rc<dyn MemStore>;
    let mem_payload = Rc::new(PlainMem::new(compact_size, 0x1));
    let compact_header: ObjPtr<CompactSpaceHeader> = unsafe { ObjPtr::new_from_addr(0x0) };
    let merkle_header: ObjPtr<MerkleHeader> = unsafe { ObjPtr::new_from_addr(CompactSpaceHeader::MSIZE) };

    mem_meta.write(
        compact_header.addr(),
        &shale::compact::CompactSpaceHeader::new(RESERVED, RESERVED).dehydrate(),
    );
    mem_meta.write(merkle_header.addr(), &MerkleHeader::new_empty().dehydrate());

    let compact_header = unsafe {
        shale::get_obj_ref(mem_meta.as_ref(), compact_header, shale::compact::CompactHeader::MSIZE)
            .unwrap()
            .to_longlive()
    };
    let merkle_header = unsafe {
        shale::get_obj_ref(mem_meta.as_ref(), merkle_header, MerkleHeader::MSIZE)
            .unwrap()
            .to_longlive()
    };

    let space = shale::compact::CompactSpace::new(mem_meta, mem_payload, compact_header, 10, 16).unwrap();
    let wctx = WriteContext::new();
    Merkle::new(merkle_header, space, &wctx)
}

fn merkle_build_test<K: AsRef<[u8]> + std::cmp::Ord + Clone, V: AsRef<[u8]> + Clone>(
    items: Vec<(K, V)>, meta_size: u64, compact_size: u64,
) -> (Merkle<shale::compact::CompactSpace>, Vec<shale::DiskWrite>) {
    let merkle = merkle_setup_test(meta_size, compact_size);
    let mut batch = merkle.new_batch();
    for (k, v) in items.iter() {
        batch.insert(k, v.as_ref().to_vec()).unwrap();
    }
    let writes = batch.commit();
    let merkle_root = &*merkle.root_hash();
    let items_copy = items.clone();
    let reference_root = triehash::trie_root::<keccak_hasher::KeccakHasher, _, _, _>(items);
    println!(
        "ours: {}, correct: {}",
        hex::encode(merkle_root),
        hex::encode(reference_root)
    );
    if merkle_root != &reference_root {
        for (k, v) in items_copy {
            println!("{} => {}", hex::encode(k), hex::encode(v));
        }
        merkle.dump();
        panic!();
    }
    (merkle, writes)
}

#[test]
fn test_root_hash_simple_insertions() {
    let items = vec![
        ("do", "verb"),
        ("doe", "reindeer"),
        ("dog", "puppy"),
        ("doge", "coin"),
        ("horse", "stallion"),
        ("ddd", "ok"),
    ];
    let (merkle, writes) = merkle_build_test(items, 0x10000, 0x10000);
    for w in writes {
        println!("{:?}", w);
    }
    merkle.dump();
}

#[test]
fn test_root_hash_fuzz_insertions() {
    use rand::{rngs::StdRng, Rng, SeedableRng};
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
    for _ in 0..1000 {
        let mut items = Vec::new();
        for _ in 0..100 {
            let val: Vec<u8> = (0..8).map(|_| rng.borrow_mut().gen()).collect();
            items.push((keygen(), val));
        }
        merkle_build_test(items, 0x100000, 0x100000);
    }
}

#[test]
fn test_root_hash_reversed_deletions() {
    use rand::{rngs::StdRng, Rng, SeedableRng};
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
    for i in 0..1000 {
        let mut items = std::collections::HashMap::new();
        for _ in 0..100 {
            let val: Vec<u8> = (0..8).map(|_| rng.borrow_mut().gen()).collect();
            items.insert(keygen(), val);
        }
        let mut items: Vec<_> = items.into_iter().collect();
        items.sort();
        let merkle = merkle_setup_test(0x100000, 0x100000);
        let mut hashes = Vec::new();
        let mut dumps = Vec::new();
        for (k, v) in items.iter() {
            dumps.push(merkle.dump());
            let mut wb = merkle.new_batch();
            wb.insert(k, v.to_vec()).unwrap();
            wb.commit();
            hashes.push(merkle.root_hash());
        }
        hashes.pop();
        println!("----");
        let mut prev_dump = merkle.dump();
        for (((k, _), h), d) in items.iter().rev().zip(hashes.iter().rev()).zip(dumps.iter().rev()) {
            let mut wb = merkle.new_batch();
            wb.remove(k).unwrap();
            wb.commit();
            let h0 = merkle.root_hash();
            if *h != h0 {
                for (k, _) in items.iter() {
                    println!("{}", hex::encode(k));
                }
                println!("{} != {}", hex::encode(&**h), hex::encode(&*h0));
                println!("== before {} ===", hex::encode(k));
                print!("{}", prev_dump);
                println!("== after {} ===", hex::encode(k));
                print!("{}", merkle.dump());
                println!("== should be ===");
                print!("{}", d);
                panic!();
            }
            prev_dump = merkle.dump();
        }
        println!("i = {}", i);
    }
}

#[test]
fn test_root_hash_random_deletions() {
    use rand::{rngs::StdRng, seq::SliceRandom, Rng, SeedableRng};
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
    for i in 0..1000 {
        let mut items = std::collections::HashMap::new();
        for _ in 0..100 {
            let val: Vec<u8> = (0..8).map(|_| rng.borrow_mut().gen()).collect();
            items.insert(keygen(), val);
        }
        let mut items_ordered: Vec<_> = items.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
        items_ordered.sort();
        items_ordered.shuffle(&mut *rng.borrow_mut());
        let merkle = merkle_setup_test(0x100000, 0x100000);
        for (k, v) in items.iter() {
            let mut wb = merkle.new_batch();
            wb.insert(k, v.to_vec()).unwrap();
            wb.commit();
        }
        for (k, _) in items_ordered.into_iter() {
            let mut wb = merkle.new_batch();
            wb.remove(&k).unwrap();
            wb.commit();
            items.remove(&k);
            let h = triehash::trie_root::<keccak_hasher::KeccakHasher, Vec<_>, _, _>(items.iter().collect());
            let h0 = merkle.root_hash();
            if &h[..] != &*h0 {
                println!("{} != {}", hex::encode(h), hex::encode(&*h0));
            }
        }
        println!("i = {}", i);
    }
}
