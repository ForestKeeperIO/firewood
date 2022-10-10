use std::collections::VecDeque;
use std::rc::Rc;
use std::thread::JoinHandle;

use parking_lot::{Mutex, MutexGuard};
use shale::{compact::CompactSpaceHeader, MemStore, MummyItem, MummyObj, ObjPtr, SpaceID};
use typed_builder::TypedBuilder;

use crate::file;
use crate::merkle::{Hash, Merkle, MerkleHeader};
use crate::storage::{CachedSpace, DiskBuffer, MemStoreR, StoreConfig, StoreRevMut, StoreRevShared};
pub use crate::storage::{DiskBufferConfig, WALConfig};

const MERKLE_META_SPACE: SpaceID = 0x0;
const MERKLE_COMPACT_SPACE: SpaceID = 0x1;
const SPACE_RESERVED: u64 = 0x1000;

#[derive(Debug)]
pub enum DBError {
    InvalidParams,
    Merkle(crate::merkle::MerkleError),
    System(nix::Error),
}

#[repr(C)]
struct DBHeader {
    magic: [u8; 16],
    meta_file_nbit: u64,
    compact_file_nbit: u64,
    compact_regn_nbit: u64,
    wal_file_nbit: u64,
    wal_block_nbit: u64,
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
    #[builder(default = 65536)]
    merkle_ncached_objs: usize,
    #[builder(default = DiskBufferConfig::builder().build())]
    buffer: DiskBufferConfig,
    #[builder(default = WALConfig::builder().build())]
    wal: WALConfig,
}

struct DBSpace<T> {
    meta: T,
    payload: T,
}

impl<T> DBSpace<T> {
    fn new(meta: T, payload: T) -> Self {
        Self { meta, payload }
    }
}

struct DBInner {
    merkle: Merkle,
    disk_requester: crate::storage::DiskBufferRequester,
    disk_thread: Option<JoinHandle<()>>,
    staging: DBSpace<Rc<StoreRevMut>>,
    cached: DBSpace<Rc<CachedSpace>>,
    revisions: VecDeque<DBSpace<StoreRevShared>>,
    max_revisions: usize,
}

impl Drop for DBInner {
    fn drop(&mut self) {
        self.disk_requester.shutdown();
        self.disk_thread.take().map(JoinHandle::join);
    }
}

pub struct DB {
    inner: Mutex<DBInner>,
    compact_regn_nbit: u64,
    merkle_ncached_objs: usize,
}

impl DB {
    pub fn new(db_path: &str, cfg: &DBConfig) -> Result<Self, DBError> {
        // TODO: make sure all fds are released at the end
        if cfg.truncate {
            let _ = std::fs::remove_dir_all(db_path);
        }
        let (db_fd, reset) = file::open_dir(db_path, cfg.truncate).map_err(DBError::System)?;

        let merkle_fd = file::touch_dir("merkle", db_fd).map_err(DBError::System)?;
        let merkle_meta_fd = file::touch_dir("meta", merkle_fd).map_err(DBError::System)?;
        let merkle_compact_fd = file::touch_dir("compact", merkle_fd).map_err(DBError::System)?;

        let blob_fd = file::touch_dir("blob", db_fd).map_err(DBError::System)?;
        let blob_meta_fd = file::touch_dir("meta", blob_fd).map_err(DBError::System)?;
        let blob_compact_fd = file::touch_dir("compact", blob_fd).map_err(DBError::System)?;

        let file0 = crate::file::File::new(0, SPACE_RESERVED, merkle_meta_fd).map_err(DBError::System)?;
        let fd0 = file0.get_fd();

        if reset {
            if cfg.compact_file_nbit < cfg.compact_regn_nbit || cfg.compact_regn_nbit < crate::storage::PAGE_SIZE_NBIT {
                return Err(DBError::InvalidParams)
            }
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
                wal_file_nbit: cfg.wal.file_nbit,
                wal_block_nbit: cfg.wal.block_nbit,
            };
            nix::sys::uio::pwrite(fd0, &shale::util::get_raw_bytes(&header), 0).map_err(DBError::System)?;
        }

        // read DBHeader
        let mut header_bytes = [0; std::mem::size_of::<DBHeader>()];
        nix::sys::uio::pread(fd0, &mut header_bytes, 0).map_err(DBError::System)?;
        drop(file0);
        let mut offset = header_bytes.len() as u64;
        let header = unsafe { std::mem::transmute::<_, DBHeader>(header_bytes) };

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

        // setup disk buffer
        let cached = DBSpace::new(
            Rc::new(
                CachedSpace::new(
                    &StoreConfig::builder()
                        .ncached_pages(cfg.meta_ncached_pages)
                        .ncached_files(cfg.meta_ncached_files)
                        .space_id(MERKLE_META_SPACE)
                        .file_nbit(header.meta_file_nbit)
                        .rootfd(merkle_meta_fd)
                        .build(),
                )
                .unwrap(),
            ),
            Rc::new(
                CachedSpace::new(
                    &StoreConfig::builder()
                        .ncached_pages(cfg.compact_ncached_pages)
                        .ncached_files(cfg.compact_ncached_files)
                        .space_id(MERKLE_COMPACT_SPACE)
                        .file_nbit(header.compact_file_nbit)
                        .rootfd(merkle_compact_fd)
                        .build(),
                )
                .unwrap(),
            ),
        );

        let wal = WALConfig::builder()
            .file_nbit(header.wal_file_nbit)
            .block_nbit(header.wal_block_nbit)
            .max_revisions(cfg.wal.max_revisions)
            .build();
        let (sender, inbound) = tokio::sync::mpsc::channel(cfg.buffer.max_buffered);
        let disk_requester = crate::storage::DiskBufferRequester::new(sender);
        let buffer = cfg.buffer.clone();
        let disk_thread = Some(std::thread::spawn(move || {
            let disk_buffer = DiskBuffer::new(inbound, &buffer, &wal).unwrap();
            disk_buffer.run()
        }));
        disk_requester.reg_cached_space(cached.meta.as_ref());
        disk_requester.reg_cached_space(cached.payload.as_ref());

        let staging = DBSpace::new(
            Rc::new(StoreRevMut::new(cached.meta.clone() as Rc<dyn MemStoreR>)),
            Rc::new(StoreRevMut::new(cached.payload.clone() as Rc<dyn MemStoreR>)),
        );

        if reset {
            // initialize headers
            staging.meta.write(
                compact_header.addr(),
                &shale::compact::CompactSpaceHeader::new(SPACE_RESERVED, SPACE_RESERVED).dehydrate(),
            );
            staging
                .meta
                .write(merkle_header.addr(), &MerkleHeader::new_empty().dehydrate());
        }

        let staging_meta_ref = staging.meta.as_ref() as &dyn MemStore;
        let (ch_ref, mh_ref) = unsafe {
            (
                MummyObj::ptr_to_obj(staging_meta_ref, compact_header, shale::compact::CompactHeader::MSIZE).unwrap(),
                MummyObj::ptr_to_obj(staging_meta_ref, merkle_header, MerkleHeader::MSIZE).unwrap(),
            )
        };

        let cache = shale::ObjCache::new(cfg.merkle_ncached_objs);
        let space = shale::compact::CompactSpace::new(
            staging.meta.clone(),
            staging.payload.clone(),
            ch_ref,
            cache,
            cfg.compact_max_walk,
            header.compact_regn_nbit,
        )
        .unwrap();
        disk_requester.init_wal("wal", db_fd);
        let merkle = Merkle::new(mh_ref, Box::new(space), false).unwrap();
        Ok(Self {
            inner: Mutex::new(DBInner {
                merkle,
                disk_thread,
                disk_requester,
                staging,
                cached,
                revisions: VecDeque::new(),
                max_revisions: cfg.wal.max_revisions as usize,
            }),
            compact_regn_nbit: header.compact_regn_nbit,
            merkle_ncached_objs: cfg.merkle_ncached_objs,
        })
    }

    pub fn new_writebatch(&self) -> WriteBatch {
        WriteBatch {
            m: self.inner.lock(),
            committed: false,
        }
    }

    pub fn root_hash(&self) -> Hash {
        self.inner.lock().merkle.root_hash()
    }

    pub fn dump(&self) -> String {
        self.inner.lock().merkle.dump()
    }

    pub fn get(&self, key: &[u8]) -> Result<Vec<u8>, DBError> {
        self.inner.lock().merkle.get(key).map_err(DBError::Merkle)
    }

    pub fn get_revision(&self, nback: usize, ncached_objs: Option<usize>) -> Option<Revision> {
        let mut inner = self.inner.lock();
        let rlen = inner.revisions.len();
        if nback == 0 || nback > inner.max_revisions {
            return None
        }
        if rlen < nback {
            let ashes = inner.disk_requester.collect_ash(nback);
            for mut ash in ashes.into_iter().skip(rlen) {
                let (meta, payload): (Rc<dyn MemStoreR>, Rc<dyn MemStoreR>) = match inner.revisions.back() {
                    None => (inner.cached.meta.clone(), inner.cached.payload.clone()),
                    Some(s) => (s.meta.inner().clone(), s.payload.inner().clone()),
                };
                for (_, a) in ash.0.iter_mut() {
                    a.old.reverse()
                }

                inner.revisions.push_back(DBSpace::new(
                    StoreRevShared::from_ash(meta, &ash.0[&MERKLE_META_SPACE].old).unwrap(),
                    StoreRevShared::from_ash(payload, &ash.0[&MERKLE_COMPACT_SPACE].old).unwrap(),
                ));
            }
        }
        if inner.revisions.len() < nback {
            return None
        }
        // set up the storage layout
        let compact_header: ObjPtr<CompactSpaceHeader>;
        let merkle_header: ObjPtr<MerkleHeader>;
        unsafe {
            let mut offset = std::mem::size_of::<DBHeader>() as u64;
            // CompactHeader starts after DBHeader in meta space
            compact_header = ObjPtr::new_from_addr(offset);
            offset += CompactSpaceHeader::MSIZE;
            // MerkleHeader starts after CompactHeader in meta space
            merkle_header = ObjPtr::new_from_addr(offset);
        }

        let space = &inner.revisions[nback - 1];
        let meta_ref = &space.meta as &dyn MemStore;
        let (ch_ref, mh_ref) = unsafe {
            (
                MummyObj::ptr_to_obj(meta_ref, compact_header, shale::compact::CompactHeader::MSIZE).unwrap(),
                MummyObj::ptr_to_obj(meta_ref, merkle_header, MerkleHeader::MSIZE).unwrap(),
            )
        };

        let cache = shale::ObjCache::new(ncached_objs.unwrap_or(self.merkle_ncached_objs));
        let space = shale::compact::CompactSpace::new(
            Rc::new(space.meta.clone()),
            Rc::new(space.payload.clone()),
            ch_ref,
            cache,
            0,
            self.compact_regn_nbit,
        )
        .unwrap();
        Some(Revision {
            _m: inner,
            merkle: Merkle::new(mh_ref, Box::new(space), true)?,
        })
    }
}

pub struct Revision<'a> {
    _m: MutexGuard<'a, DBInner>,
    merkle: Merkle,
}

impl<'a> Revision<'a> {
    pub fn root_hash(&self) -> Hash {
        self.merkle.root_hash()
    }

    pub fn dump(&self) -> String {
        self.merkle.dump()
    }

    pub fn get(&self, key: &[u8]) -> Result<Vec<u8>, DBError> {
        self.merkle.get(key).map_err(DBError::Merkle)
    }
}

pub struct WriteBatch<'a> {
    m: MutexGuard<'a, DBInner>,
    committed: bool,
}

impl<'a> WriteBatch<'a> {
    pub fn insert<K: AsRef<[u8]>>(&mut self, key: K, val: Vec<u8>) -> Result<(), DBError> {
        self.m.merkle.insert(key, val).map_err(DBError::Merkle)
    }

    pub fn remove<K: AsRef<[u8]>>(&mut self, key: K) -> Result<bool, DBError> {
        self.m.merkle.remove(key).map_err(DBError::Merkle)
    }

    pub fn commit(mut self) {
        use crate::storage::BufferWrite;
        let inner = &mut *self.m;
        // clear the staging layer and apply changes to the CachedSpace
        let (payload_pages, payload_plain) = inner.staging.payload.take_delta();
        let (meta_pages, meta_plain) = inner.staging.meta.take_delta();

        let old_meta_delta = inner.cached.meta.update(&meta_pages).unwrap();
        let old_payload_delta = inner.cached.payload.update(&payload_pages).unwrap();

        // update the rolling window of past revisions
        let new_base = DBSpace::new(
            StoreRevShared::from_delta(inner.cached.meta.clone(), old_meta_delta),
            StoreRevShared::from_delta(inner.cached.payload.clone(), old_payload_delta),
        );

        if let Some(rev) = inner.revisions.front_mut() {
            rev.meta.set_prev(new_base.meta.inner().clone());
            rev.payload.set_prev(new_base.payload.inner().clone());
        }
        inner.revisions.push_front(new_base);
        while inner.revisions.len() > inner.max_revisions {
            inner.revisions.pop_back();
        }

        self.committed = true;

        // schedule writes to the disk
        inner.disk_requester.write(
            vec![
                BufferWrite {
                    space_id: inner.staging.payload.id(),
                    delta: payload_pages,
                },
                BufferWrite {
                    space_id: inner.staging.meta.id(),
                    delta: meta_pages,
                },
            ],
            crate::storage::AshRecord([(MERKLE_COMPACT_SPACE, payload_plain), (MERKLE_META_SPACE, meta_plain)].into()),
        );
    }
}

impl<'a> Drop for WriteBatch<'a> {
    fn drop(&mut self) {
        if !self.committed {
            // drop the staging changes
            self.m.staging.payload.take_delta();
            self.m.staging.meta.take_delta();
        }
    }
}
