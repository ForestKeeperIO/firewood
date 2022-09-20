use std::rc::Rc;
use std::sync::Arc;
use std::thread::JoinHandle;

use parking_lot::{Mutex, MutexGuard};
use shale::{MemStore, MummyItem, ObjPtr, SpaceID};
use typed_builder::TypedBuilder;

use crate::file;
use crate::merkle::{Merkle, MerkleHeader};
use crate::storage::{CachedSpace, DiskBuffer, DiskBufferConfig, MemStoreR, StoreConfig, StoreRevMut};

const MERKLE_META_SPACE: SpaceID = 0x0;
const MERKLE_COMPACT_SPACE: SpaceID = 0x1;
const SPACE_RESERVED: u64 = 0x1000;

#[derive(Debug)]
pub enum DBError {
    Merkle(crate::merkle::MerkleError),
}

#[derive(TypedBuilder)]
pub struct MerkleDBConfig {
    meta_ncached_pages: usize,
    meta_ncached_files: usize,
    #[builder(default = 22)] // 4MB file by default
    meta_file_nbit: u64,
    compact_ncached_pages: usize,
    compact_ncached_files: usize,
    #[builder(default = 22)] // 4MB file by default
    compact_file_nbit: u64,
    #[builder(default = 10)]
    compact_max_walk: u64,
    #[builder(default = 16)]
    compact_regn_nbit: u64,
    #[builder(default = false)]
    truncate: bool,
}

struct MerkleDBInner {
    merkle: Merkle<shale::compact::CompactSpace>,
    disk_requester: crate::storage::DiskBufferRequester,
    disk_thread: Option<JoinHandle<()>>,
    mem_meta: Rc<StoreRevMut>,
    mem_payload: Rc<StoreRevMut>,
}

impl Drop for MerkleDBInner {
    fn drop(&mut self) {
        self.disk_requester.shutdown();
        self.disk_thread.take().map(JoinHandle::join);
    }
}

pub struct MerkleDB(Mutex<MerkleDBInner>);

impl MerkleDB {
    pub fn new(db_path: &str, cfg: &MerkleDBConfig) -> Self {
        use shale::compact::CompactSpaceHeader;
        let (fd0, reset) = file::open_dir(db_path, cfg.truncate).unwrap();
        let freed_fd = file::touch_dir("freed", fd0).unwrap();
        let compact_fd = file::touch_dir("compact", fd0).unwrap();

        let mem_meta = Arc::new(
            CachedSpace::new(
                &StoreConfig::builder()
                    .ncached_pages(cfg.meta_ncached_pages)
                    .ncached_files(cfg.meta_ncached_files)
                    .space_id(MERKLE_META_SPACE)
                    .file_nbit(cfg.meta_file_nbit)
                    .rootfd(freed_fd)
                    .build(),
            )
            .unwrap(),
        );
        let mem_payload = Arc::new(
            CachedSpace::new(
                &StoreConfig::builder()
                    .ncached_pages(cfg.compact_ncached_pages)
                    .ncached_files(cfg.compact_ncached_files)
                    .space_id(MERKLE_COMPACT_SPACE)
                    .file_nbit(cfg.compact_file_nbit)
                    .rootfd(compact_fd)
                    .build(),
            )
            .unwrap(),
        );

        let mut disk_buffer = DiskBuffer::new(&DiskBufferConfig::builder().build()).unwrap();
        disk_buffer.reg_cached_space(mem_meta.as_ref());
        disk_buffer.reg_cached_space(mem_payload.as_ref());
        let disk_requester = disk_buffer.requester().clone();
        let disk_thread = Some(std::thread::spawn(move || disk_buffer.run()));

        let mem_meta = Rc::new(StoreRevMut::new(mem_meta as Arc<dyn MemStoreR>));
        let mem_payload = Rc::new(StoreRevMut::new(mem_payload as Arc<dyn MemStoreR>));

        // set up the storage layout

        let compact_header: ObjPtr<CompactSpaceHeader> = unsafe { ObjPtr::new_from_addr(0x0) };
        let merkle_header: ObjPtr<MerkleHeader> = unsafe { ObjPtr::new_from_addr(CompactSpaceHeader::MSIZE) };

        if reset {
            // initialize the DB
            mem_meta.write(
                compact_header.addr(),
                &shale::compact::CompactSpaceHeader::new(SPACE_RESERVED, SPACE_RESERVED).dehydrate(),
            );
            mem_meta.write(merkle_header.addr(), &MerkleHeader::new_empty().dehydrate());
        }

        let mem_meta_ref = mem_meta.as_ref() as &dyn MemStore;
        let compact_header = unsafe {
            shale::get_obj_ref(mem_meta_ref, compact_header, shale::compact::CompactHeader::MSIZE)
                .unwrap()
                .to_longlive()
        };
        let merkle_header = unsafe {
            shale::get_obj_ref(mem_meta_ref, merkle_header, MerkleHeader::MSIZE)
                .unwrap()
                .to_longlive()
        };

        let space = shale::compact::CompactSpace::new(
            mem_meta.clone(),
            mem_payload.clone(),
            compact_header,
            cfg.compact_max_walk,
            cfg.compact_regn_nbit,
        )
        .unwrap();
        let merkle = Merkle::new(merkle_header, space);
        Self(Mutex::new(MerkleDBInner {
            merkle,
            disk_thread,
            disk_requester,
            mem_meta,
            mem_payload,
        }))
    }

    pub fn new_writebatch(&self) -> WriteBatch {
        WriteBatch {
            m: self.0.lock(),
        }
    }
}

pub struct WriteBatch<'a> {
    m: MutexGuard<'a, MerkleDBInner>,
}

impl<'a> WriteBatch<'a> {
    pub fn insert<K: AsRef<[u8]>>(&mut self, key: K, val: Vec<u8>) -> Result<(), DBError> {
        self.m.merkle.insert(key, val).map_err(DBError::Merkle)
    }

    pub fn remove<K: AsRef<[u8]>>(&mut self, key: K) -> Result<bool, DBError> {
        self.m.merkle.remove(key).map_err(DBError::Merkle)
    }

    pub fn commit(self) {
        use crate::storage::BufferWrite;
        let inner = self.m;
        inner.disk_requester.write(vec![
            BufferWrite {
                space_id: inner.mem_payload.id(),
                delta: inner.mem_payload.to_delta(),
            },
            BufferWrite {
                space_id: inner.mem_meta.id(),
                delta: inner.mem_meta.to_delta(),
            },
        ]);
    }
}

