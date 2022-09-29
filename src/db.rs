use std::rc::Rc;
use std::thread::JoinHandle;

use parking_lot::{Mutex, MutexGuard};
use shale::{MemStore, MummyItem, ObjPtr, SpaceID};
use typed_builder::TypedBuilder;

use crate::file;
use crate::merkle::{Hash, Merkle, MerkleHeader};
use crate::storage::{CachedSpace, DiskBuffer, MemStoreR, StoreConfig, StoreRevMut};
pub use crate::storage::DiskBufferConfig;

const MERKLE_META_SPACE: SpaceID = 0x0;
const MERKLE_COMPACT_SPACE: SpaceID = 0x1;
const SPACE_RESERVED: u64 = 0x1000;

#[derive(Debug)]
pub enum DBError {
    Merkle(crate::merkle::MerkleError),
    System(nix::Error),
}

#[repr(C)]
struct DBHeader {
    magic: [u8; 16],
    meta_file_nbit: u64,
    compact_file_nbit: u64,
    compact_regn_nbit: u64,
}

#[derive(TypedBuilder)]
pub struct DBConfig {
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
    #[builder(default = DiskBufferConfig::builder().build())]
    buffer: DiskBufferConfig,
}

struct DBInner {
    merkle: Merkle<shale::compact::CompactSpace>,
    disk_requester: crate::storage::DiskBufferRequester,
    disk_thread: Option<JoinHandle<()>>,
    staging_meta: Rc<StoreRevMut>,
    staging_payload: Rc<StoreRevMut>,
    cached_meta: Rc<CachedSpace>,
    cached_payload: Rc<CachedSpace>,
}

impl Drop for DBInner {
    fn drop(&mut self) {
        self.disk_requester.shutdown();
        self.disk_thread.take().map(JoinHandle::join);
    }
}

pub struct DB(Mutex<DBInner>);

impl DB {
    pub fn new(db_path: &str, cfg: &DBConfig) -> Result<Self, DBError> {
        use shale::compact::CompactSpaceHeader;
        if cfg.truncate {
            let _ = std::fs::remove_dir_all(db_path);
        }
        let (db_fd, reset) = file::open_dir(db_path, cfg.truncate).map_err(DBError::System)?;
        let meta_fd = file::touch_dir("meta", db_fd).map_err(DBError::System)?;
        let compact_fd = file::touch_dir("compact", db_fd).map_err(DBError::System)?;

        let file0 = crate::file::File::new(0, SPACE_RESERVED, meta_fd).map_err(DBError::System)?;
        let fd0 = file0.get_fd();

        if reset {
            // FIXME: return as an error
            assert!(cfg.compact_file_nbit >= cfg.compact_regn_nbit);
            assert!(cfg.compact_regn_nbit >= crate::storage::PAGE_SIZE_NBIT);
            nix::unistd::ftruncate(fd0, 0).map_err(DBError::System)?;
            nix::unistd::ftruncate(fd0, 1 << cfg.meta_file_nbit).map_err(DBError::System)?;
            let magic_str = b"firewood v0.1";
            let mut magic = [0; 16];
            magic[..magic_str.len()].copy_from_slice(magic_str);
            let header = DBHeader {
                magic: magic,
                meta_file_nbit: cfg.meta_file_nbit,
                compact_file_nbit: cfg.compact_file_nbit,
                compact_regn_nbit: cfg.compact_regn_nbit,
            };
            nix::sys::uio::pwrite(fd0, &shale::util::get_raw_bytes(&header), 0).map_err(DBError::System)?;
        }

        // read DBHeader
        let mut header_bytes = [0; std::mem::size_of::<DBHeader>()];
        nix::sys::uio::pread(fd0, &mut header_bytes, 0).map_err(DBError::System)?;
        drop(file0);
        let mut offset = header_bytes.len() as u64;
        let header = unsafe { std::mem::transmute::<_, DBHeader>(header_bytes) };

        let cached_meta = Rc::new(
            CachedSpace::new(
                &StoreConfig::builder()
                    .ncached_pages(cfg.meta_ncached_pages)
                    .ncached_files(cfg.meta_ncached_files)
                    .space_id(MERKLE_META_SPACE)
                    .file_nbit(header.meta_file_nbit)
                    .rootfd(meta_fd)
                    .build(),
            )
            .unwrap(),
        );
        let cached_payload = Rc::new(
            CachedSpace::new(
                &StoreConfig::builder()
                    .ncached_pages(cfg.compact_ncached_pages)
                    .ncached_files(cfg.compact_ncached_files)
                    .space_id(MERKLE_COMPACT_SPACE)
                    .file_nbit(header.compact_file_nbit)
                    .rootfd(compact_fd)
                    .build(),
            )
            .unwrap(),
        );

        let mut disk_buffer = DiskBuffer::new(&cfg.buffer).unwrap();
        disk_buffer.reg_cached_space(cached_meta.as_ref());
        disk_buffer.reg_cached_space(cached_payload.as_ref());
        let disk_requester = disk_buffer.requester().clone();
        let disk_thread = Some(std::thread::spawn(move || disk_buffer.run()));

        let staging_meta = Rc::new(StoreRevMut::new(cached_meta.clone() as Rc<dyn MemStoreR>));
        let staging_payload = Rc::new(StoreRevMut::new(cached_payload.clone() as Rc<dyn MemStoreR>));

        // set up the storage layout
        let compact_header: ObjPtr<CompactSpaceHeader>;
        let merkle_header: ObjPtr<MerkleHeader>;
        unsafe {
            // CompactHeader starts after DBHeader in meta space
            compact_header = ObjPtr::new_from_addr(offset);
            offset += CompactSpaceHeader::MSIZE;
            // MerkleHeader starts after CompactHeader in meta space
            merkle_header = ObjPtr::new_from_addr(offset);
            offset += MerkleHeader::MSIZE;
            assert!(offset <= SPACE_RESERVED);
        }

        if reset {
            // initialize headers
            staging_meta.write(
                compact_header.addr(),
                &shale::compact::CompactSpaceHeader::new(SPACE_RESERVED, SPACE_RESERVED).dehydrate(),
            );
            staging_meta.write(merkle_header.addr(), &MerkleHeader::new_empty().dehydrate());
        }

        let staging_meta_ref = staging_meta.as_ref() as &dyn MemStore;
        let ch_ref;
        let mh_ref;
        unsafe {
            ch_ref = shale::get_obj_ref(staging_meta_ref, compact_header, shale::compact::CompactHeader::MSIZE)
                .unwrap()
                .to_longlive();
            mh_ref = shale::get_obj_ref(staging_meta_ref, merkle_header, MerkleHeader::MSIZE)
                .unwrap()
                .to_longlive();
        }

        let space = shale::compact::CompactSpace::new(
            staging_meta.clone(),
            staging_payload.clone(),
            ch_ref,
            cfg.compact_max_walk,
            header.compact_regn_nbit,
        )
        .unwrap();
        disk_requester.init_wal("wal", db_fd);
        let merkle = Merkle::new(mh_ref, space);
        Ok(Self(Mutex::new(DBInner {
            merkle,
            disk_thread,
            disk_requester,
            staging_meta,
            staging_payload,
            cached_meta,
            cached_payload,
        })))
    }

    pub fn new_writebatch(&self) -> WriteBatch {
        WriteBatch { m: self.0.lock() }
    }

    pub fn root_hash(&self) -> Hash {
        self.0.lock().merkle.root_hash()
    }

    pub fn dump(&self) -> String {
        self.0.lock().merkle.dump()
    }
}

impl Drop for DB {
    fn drop(&mut self) {
        let inner = self.0.lock();
        println!("{} {}", inner.staging_meta.get_counter(), inner.staging_payload.get_counter());
    }
}

pub struct WriteBatch<'a> {
    m: MutexGuard<'a, DBInner>,
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
        // clear the staging layer and apply changes to the CachedSpace
        let (payload_pages, payload_plain) = inner.staging_payload.take_delta();
        let (meta_pages, meta_plain) = inner.staging_meta.take_delta();

        inner.cached_payload.update(&payload_pages);
        inner.cached_meta.update(&meta_pages);

        inner.disk_requester.write(
            vec![
                BufferWrite {
                    space_id: inner.staging_payload.id(),
                    delta: payload_pages,
                },
                BufferWrite {
                    space_id: inner.staging_meta.id(),
                    delta: meta_pages,
                },
            ],
            crate::storage::AshRecord(vec![payload_plain, meta_plain])
        );
    }

    pub fn abort(self) {
        // drop the staging changes
        self.m.staging_payload.take_delta();
        self.m.staging_meta.take_delta();
    }
}
