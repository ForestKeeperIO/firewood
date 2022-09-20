use firewood::file;
use firewood::merkle::*;
use firewood::storage::*;
use shale::{MemStore, MummyItem, ObjPtr};
use std::rc::Rc;
use std::sync::Arc;

#[test]
fn test_persistent_merkle_simple() {
    use shale::compact::CompactSpaceHeader;
    const RESERVED: u64 = 0x1000;
    let (fd0, _) = file::open_dir("persistent_merkle_simple", true).unwrap();
    let freed_fd = file::touch_dir("freed", fd0).unwrap();
    let compact_fd = file::touch_dir("compact", fd0).unwrap();

    let mem_meta = Arc::new(
        CachedSpace::new(
            &StoreConfig::builder()
                .ncached_pages(1024)
                .ncached_files(1024)
                .space_id(0x0)
                .rootfd(freed_fd)
                .build(),
        )
        .unwrap(),
    );
    let mem_payload = Arc::new(
        CachedSpace::new(
            &StoreConfig::builder()
                .ncached_pages(1024)
                .ncached_files(1024)
                .space_id(0x1)
                .rootfd(compact_fd)
                .build(),
        )
        .unwrap(),
    );

    let mut disk_buffer = DiskBuffer::new(&DiskBufferConfig::builder().build()).unwrap();
    disk_buffer.reg_cached_space(mem_meta.as_ref());
    disk_buffer.reg_cached_space(mem_payload.as_ref());
    let disk_requester = disk_buffer.requester().clone();
    let disk_thread = std::thread::spawn(move || disk_buffer.run());

    let mem_meta = Rc::new(StoreRevMut::new(mem_meta as Arc<dyn MemStoreR>));
    let mem_payload = Rc::new(StoreRevMut::new(mem_payload as Arc<dyn MemStoreR>));

    // set up the storage layout

    let compact_header: ObjPtr<CompactSpaceHeader> = unsafe { ObjPtr::new_from_addr(0x0) };
    let merkle_header: ObjPtr<MerkleHeader> = unsafe { ObjPtr::new_from_addr(CompactSpaceHeader::MSIZE) };

    mem_meta.write(
        compact_header.addr(),
        &shale::compact::CompactSpaceHeader::new(RESERVED, RESERVED).dehydrate(),
    );
    mem_meta.write(merkle_header.addr(), &MerkleHeader::new_empty().dehydrate());

    let mem_meta1 = mem_meta.clone() as Rc<dyn MemStore>;
    let compact_header = unsafe {
        shale::get_obj_ref(mem_meta1.as_ref(), compact_header, shale::compact::CompactHeader::MSIZE)
            .unwrap()
            .to_longlive()
    };
    let merkle_header = unsafe {
        shale::get_obj_ref(mem_meta1.as_ref(), merkle_header, MerkleHeader::MSIZE)
            .unwrap()
            .to_longlive()
    };

    let space =
        shale::compact::CompactSpace::new(mem_meta.clone(), mem_payload.clone(), compact_header, 10, 16).unwrap();
    let merkle = Merkle::new(merkle_header, space);
    let items = vec![
        ("do", "verb"),
        ("doe", "reindeer"),
        ("dog", "puppy"),
        ("doge", "coin"),
        ("horse", "stallion"),
        ("ddd", "ok"),
    ];
    {
        let mut wb = merkle.new_batch();
        for (k, v) in items.iter() {
            wb.insert(k, v.as_bytes().to_vec()).unwrap();
        }
    }
    disk_requester.write(vec![
        BufferWrite {
            space_id: mem_payload.id(),
            delta: mem_payload.to_delta(),
        },
        BufferWrite {
            space_id: mem_meta.id(),
            delta: mem_meta.to_delta(),
        },
    ]);

    disk_requester.shutdown();
    disk_thread.join().unwrap();

    println!("{}", merkle.dump());
}
