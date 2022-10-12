use std::collections::VecDeque;
use std::rc::Rc;
use std::thread::JoinHandle;

use parking_lot::{Mutex, MutexGuard};
use primitive_types::U256;
use shale::{compact::CompactSpaceHeader, MemStore, MummyItem, MummyObj, ObjPtr, SpaceID};
use typed_builder::TypedBuilder;

use crate::account::{Account, AccountRLP, Blob, BlobStash};
use crate::file;
use crate::merkle::{Hash, IdTrans, Merkle, MerkleError, Node};
use crate::storage::{CachedSpace, DiskBuffer, MemStoreR, SpaceWrite, StoreConfig, StoreRevMut, StoreRevShared};
pub use crate::storage::{DiskBufferConfig, WALConfig};

const MERKLE_META_SPACE: SpaceID = 0x0;
const MERKLE_COMPACT_SPACE: SpaceID = 0x1;
const BLOB_META_SPACE: SpaceID = 0x2;
const BLOB_COMPACT_SPACE: SpaceID = 0x3;
const SPACE_RESERVED: u64 = 0x1000;

const MAGIC_STR: &[u8; 13] = b"firewood v0.1";

#[derive(Debug)]
pub enum DBError {
    InvalidParams,
    Merkle(MerkleError),
    Blob(crate::account::BlobError),
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

#[derive(TypedBuilder, Clone)]
pub struct DBRevConfig {
    #[builder(default = 65536)]
    merkle_ncached_objs: usize,
    #[builder(default = 4096)]
    blob_ncached_objs: usize,
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
    #[builder(default = DBRevConfig::builder().build())]
    rev: DBRevConfig,
    #[builder(default = DiskBufferConfig::builder().build())]
    buffer: DiskBufferConfig,
    #[builder(default = WALConfig::builder().build())]
    wal: WALConfig,
}

struct SubSpace<T> {
    meta: T,
    payload: T,
}

impl<T> SubSpace<T> {
    fn new(meta: T, payload: T) -> Self {
        Self { meta, payload }
    }
}

impl SubSpace<StoreRevShared> {
    fn to_mem_store_r(&self) -> SubSpace<Rc<dyn MemStoreR>> {
        SubSpace {
            meta: self.meta.inner().clone(),
            payload: self.payload.inner().clone(),
        }
    }
}

impl SubSpace<Rc<dyn MemStoreR>> {
    fn rewind(&self, meta_writes: &[SpaceWrite], payload_writes: &[SpaceWrite]) -> SubSpace<StoreRevShared> {
        SubSpace::new(
            StoreRevShared::from_ash(self.meta.clone(), &meta_writes),
            StoreRevShared::from_ash(self.payload.clone(), &payload_writes),
        )
    }
}

impl SubSpace<Rc<CachedSpace>> {
    fn to_mem_store_r(&self) -> SubSpace<Rc<dyn MemStoreR>> {
        SubSpace {
            meta: self.meta.clone(),
            payload: self.payload.clone(),
        }
    }
}

pub struct MerkleHeader {
    acc_root: ObjPtr<Node>,
    kv_root: ObjPtr<Node>,
}

impl MerkleHeader {
    pub const MSIZE: u64 = 16;

    pub fn new_empty() -> Self {
        Self {
            acc_root: ObjPtr::null(),
            kv_root: ObjPtr::null(),
        }
    }
}

impl MummyItem for MerkleHeader {
    fn hydrate(addr: u64, mem: &dyn MemStore) -> Result<(u64, Self), shale::ShaleError> {
        let raw = mem
            .get_view(addr, Self::MSIZE)
            .ok_or(shale::ShaleError::LinearMemStoreError)?;
        let acc_root = u64::from_le_bytes(raw[..8].try_into().unwrap());
        let kv_root = u64::from_le_bytes(raw[8..].try_into().unwrap());
        unsafe {
            Ok((
                Self::MSIZE,
                Self {
                    acc_root: ObjPtr::new_from_addr(acc_root),
                    kv_root: ObjPtr::new_from_addr(kv_root),
                },
            ))
        }
    }

    fn dehydrate(&self) -> Vec<u8> {
        let mut m = Vec::new();
        m.extend(self.acc_root.addr().to_le_bytes());
        m.extend(self.kv_root.addr().to_le_bytes());
        assert_eq!(m.len() as u64, Self::MSIZE);
        m
    }
}

struct Universe<T> {
    merkle: SubSpace<T>,
    blob: SubSpace<T>,
}

impl Universe<StoreRevShared> {
    fn to_mem_store_r(&self) -> Universe<Rc<dyn MemStoreR>> {
        Universe {
            merkle: self.merkle.to_mem_store_r(),
            blob: self.blob.to_mem_store_r(),
        }
    }
}

impl Universe<Rc<CachedSpace>> {
    fn to_mem_store_r(&self) -> Universe<Rc<dyn MemStoreR>> {
        Universe {
            merkle: self.merkle.to_mem_store_r(),
            blob: self.blob.to_mem_store_r(),
        }
    }
}

impl Universe<Rc<dyn MemStoreR>> {
    fn rewind(
        &self, merkle_meta_writes: &[SpaceWrite], merkle_payload_writes: &[SpaceWrite],
        blob_meta_writes: &[SpaceWrite], blob_payload_writes: &[SpaceWrite],
    ) -> Universe<StoreRevShared> {
        Universe {
            merkle: self.merkle.rewind(merkle_meta_writes, merkle_payload_writes),
            blob: self.blob.rewind(blob_meta_writes, blob_payload_writes),
        }
    }
}

pub struct DBRev {
    header: shale::Obj<MerkleHeader>,
    merkle: Merkle,
    blob: BlobStash,
}

impl DBRev {
    fn borrow_split(&mut self) -> (&mut shale::Obj<MerkleHeader>, &mut Merkle, &mut BlobStash) {
        (&mut self.header, &mut self.merkle, &mut self.blob)
    }

    pub fn kv_root_hash(&self) -> Result<Hash, DBError> {
        self.merkle
            .root_hash::<IdTrans>(self.header.kv_root)
            .map_err(DBError::Merkle)
    }

    pub fn kv_dump(&self) -> String {
        self.merkle.dump(self.header.kv_root)
    }

    pub fn root_hash(&self) -> Result<Hash, DBError> {
        self.merkle
            .root_hash::<AccountRLP>(self.header.acc_root)
            .map_err(DBError::Merkle)
    }

    pub fn dump(&self) -> String {
        self.merkle.dump(self.header.acc_root)
    }

    fn get_account(&self, key: &[u8]) -> Result<Account, DBError> {
        Ok(match self.merkle.get(key, self.header.acc_root) {
            Ok(bytes) => Account::deserialize(&*bytes),
            Err(MerkleError::KeyNotFound) => Account::default(),
            Err(e) => return Err(DBError::Merkle(e)),
        })
    }

    pub fn get_balance(&self, key: &[u8]) -> Result<U256, DBError> {
        Ok(self.get_account(key)?.balance)
    }

    pub fn get_code(&self, key: &[u8]) -> Result<Vec<u8>, DBError> {
        let b = self.blob.get_blob(self.get_account(key)?.code).map_err(DBError::Blob)?;
        Ok(match &**b {
            Blob::Code(code) => code.clone(),
        })
    }

    pub fn get_nonce(&self, key: &[u8]) -> Result<u64, DBError> {
        Ok(self.get_account(key)?.nonce)
    }

    pub fn get_state(&self, key: &[u8], sub_key: &[u8]) -> Result<Vec<u8>, DBError> {
        let root = self.get_account(key)?.root;
        if root.is_null() {
            return Ok(Vec::new())
        }
        Ok(match self.merkle.get(sub_key, root) {
            Ok(v) => v.to_vec(),
            Err(MerkleError::KeyNotFound) => Vec::new(),
            Err(e) => return Err(DBError::Merkle(e)),
        })
    }

    pub fn exist(&self, key: &[u8]) -> Result<bool, DBError> {
        Ok(match self.merkle.get(key, self.header.acc_root) {
            Ok(_) => true,
            Err(MerkleError::KeyNotFound) => false,
            Err(e) => return Err(DBError::Merkle(e)),
        })
    }
}

struct DBInner {
    latest: DBRev,
    disk_requester: crate::storage::DiskBufferRequester,
    disk_thread: Option<JoinHandle<()>>,
    staging: Universe<Rc<StoreRevMut>>,
    cached: Universe<Rc<CachedSpace>>,
    revisions: VecDeque<Universe<StoreRevShared>>,
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
    rev_cfg: DBRevConfig,
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
        let merkle_payload_fd = file::touch_dir("compact", merkle_fd).map_err(DBError::System)?;

        let blob_fd = file::touch_dir("blob", db_fd).map_err(DBError::System)?;
        let blob_meta_fd = file::touch_dir("meta", blob_fd).map_err(DBError::System)?;
        let blob_payload_fd = file::touch_dir("compact", blob_fd).map_err(DBError::System)?;

        let file0 = crate::file::File::new(0, SPACE_RESERVED, merkle_meta_fd).map_err(DBError::System)?;
        let fd0 = file0.get_fd();

        if reset {
            // initialize DBHeader
            if cfg.compact_file_nbit < cfg.compact_regn_nbit || cfg.compact_regn_nbit < crate::storage::PAGE_SIZE_NBIT {
                return Err(DBError::InvalidParams)
            }
            nix::unistd::ftruncate(fd0, 0).map_err(DBError::System)?;
            nix::unistd::ftruncate(fd0, 1 << cfg.meta_file_nbit).map_err(DBError::System)?;
            let mut magic = [0; 16];
            magic[..MAGIC_STR.len()].copy_from_slice(MAGIC_STR);
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

        // setup disk buffer
        let cached = Universe {
            merkle: SubSpace::new(
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
                            .rootfd(merkle_payload_fd)
                            .build(),
                    )
                    .unwrap(),
                ),
            ),
            blob: SubSpace::new(
                Rc::new(
                    CachedSpace::new(
                        &StoreConfig::builder()
                            .ncached_pages(cfg.meta_ncached_pages)
                            .ncached_files(cfg.meta_ncached_files)
                            .space_id(BLOB_META_SPACE)
                            .file_nbit(header.meta_file_nbit)
                            .rootfd(blob_meta_fd)
                            .build(),
                    )
                    .unwrap(),
                ),
                Rc::new(
                    CachedSpace::new(
                        &StoreConfig::builder()
                            .ncached_pages(cfg.compact_ncached_pages)
                            .ncached_files(cfg.compact_ncached_files)
                            .space_id(BLOB_COMPACT_SPACE)
                            .file_nbit(header.compact_file_nbit)
                            .rootfd(blob_payload_fd)
                            .build(),
                    )
                    .unwrap(),
                ),
            ),
        };

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

        disk_requester.reg_cached_space(cached.merkle.meta.as_ref());
        disk_requester.reg_cached_space(cached.merkle.payload.as_ref());
        disk_requester.reg_cached_space(cached.blob.meta.as_ref());
        disk_requester.reg_cached_space(cached.blob.payload.as_ref());

        let staging = Universe {
            merkle: SubSpace::new(
                Rc::new(StoreRevMut::new(cached.merkle.meta.clone() as Rc<dyn MemStoreR>)),
                Rc::new(StoreRevMut::new(cached.merkle.payload.clone() as Rc<dyn MemStoreR>)),
            ),
            blob: SubSpace::new(
                Rc::new(StoreRevMut::new(cached.blob.meta.clone() as Rc<dyn MemStoreR>)),
                Rc::new(StoreRevMut::new(cached.blob.payload.clone() as Rc<dyn MemStoreR>)),
            ),
        };

        // set up the storage layout
        let merkle_payload_header: ObjPtr<CompactSpaceHeader>;
        let merkle_header: ObjPtr<MerkleHeader>;
        let blob_payload_header: ObjPtr<CompactSpaceHeader>;
        unsafe {
            // Merkle CompactHeader starts after DBHeader in merkle meta space
            merkle_payload_header = ObjPtr::new_from_addr(offset);
            offset += CompactSpaceHeader::MSIZE;
            // MerkleHeader starts after CompactHeader in merkle meta space
            merkle_header = ObjPtr::new_from_addr(offset);
            offset += MerkleHeader::MSIZE;
            assert!(offset <= SPACE_RESERVED);
            // Blob CompactSpaceHeader starts right in blob meta space
            blob_payload_header = ObjPtr::new_from_addr(0);
        }

        if reset {
            // initialize space headers
            staging.merkle.meta.write(
                merkle_payload_header.addr(),
                &shale::compact::CompactSpaceHeader::new(SPACE_RESERVED, SPACE_RESERVED).dehydrate(),
            );
            staging
                .merkle
                .meta
                .write(merkle_header.addr(), &MerkleHeader::new_empty().dehydrate());
            staging.blob.meta.write(
                blob_payload_header.addr(),
                &shale::compact::CompactSpaceHeader::new(SPACE_RESERVED, SPACE_RESERVED).dehydrate(),
            );
        }

        let (merkle_payload_header_ref, mut merkle_header_ref, blob_payload_header_ref) = unsafe {
            let merkle_meta_ref = staging.merkle.meta.as_ref() as &dyn MemStore;
            let blob_meta_ref = staging.blob.meta.as_ref() as &dyn MemStore;

            (
                MummyObj::ptr_to_obj(
                    merkle_meta_ref,
                    merkle_payload_header,
                    shale::compact::CompactHeader::MSIZE,
                )
                .unwrap(),
                MummyObj::ptr_to_obj(merkle_meta_ref, merkle_header, MerkleHeader::MSIZE).unwrap(),
                MummyObj::ptr_to_obj(blob_meta_ref, blob_payload_header, shale::compact::CompactHeader::MSIZE).unwrap(),
            )
        };

        let merkle_space = shale::compact::CompactSpace::new(
            staging.merkle.meta.clone(),
            staging.merkle.payload.clone(),
            merkle_payload_header_ref,
            shale::ObjCache::new(cfg.rev.merkle_ncached_objs),
            cfg.compact_max_walk,
            header.compact_regn_nbit,
        )
        .unwrap();

        let blob_space = shale::compact::CompactSpace::new(
            staging.blob.meta.clone(),
            staging.blob.payload.clone(),
            blob_payload_header_ref,
            shale::ObjCache::new(cfg.rev.blob_ncached_objs),
            cfg.compact_max_walk,
            header.compact_regn_nbit,
        )
        .unwrap();

        // recover from WAL
        disk_requester.init_wal("wal", db_fd);

        if merkle_header_ref.acc_root.is_null() {
            let mut err = Ok(());
            // create the sentinel node
            merkle_header_ref
                .write(|r| {
                    err = (|| {
                        Merkle::init_root(&mut r.acc_root, &merkle_space)?;
                        Merkle::init_root(&mut r.kv_root, &merkle_space)
                    })();
                })
                .unwrap();
            err.map_err(DBError::Merkle)?
        }

        let latest = DBRev {
            header: merkle_header_ref,
            merkle: Merkle::new(Box::new(merkle_space)),
            blob: BlobStash::new(Box::new(blob_space)),
        };

        Ok(Self {
            inner: Mutex::new(DBInner {
                latest,
                disk_thread,
                disk_requester,
                staging,
                cached,
                revisions: VecDeque::new(),
                max_revisions: cfg.wal.max_revisions as usize,
            }),
            compact_regn_nbit: header.compact_regn_nbit,
            rev_cfg: cfg.rev.clone(),
        })
    }

    pub fn new_writebatch(&self) -> WriteBatch {
        WriteBatch {
            m: self.inner.lock(),
            committed: false,
        }
    }

    pub fn kv_dump(&self) -> String {
        self.inner.lock().latest.kv_dump()
    }

    pub fn dump(&self) -> String {
        self.inner.lock().latest.dump()
    }

    pub fn get_balance(&self, key: &[u8]) -> Result<U256, DBError> {
        self.inner.lock().latest.get_balance(key)
    }

    pub fn get_code(&self, key: &[u8]) -> Result<Vec<u8>, DBError> {
        self.inner.lock().latest.get_code(key)
    }

    pub fn get_nonce(&self, key: &[u8]) -> Result<u64, DBError> {
        self.inner.lock().latest.get_nonce(key)
    }

    pub fn get_state(&self, key: &[u8], sub_key: &[u8]) -> Result<Vec<u8>, DBError> {
        self.inner.lock().latest.get_state(key, sub_key)
    }

    pub fn exist(&self, key: &[u8]) -> Result<bool, DBError> {
        self.inner.lock().latest.exist(key)
    }

    pub fn get_revision(&self, nback: usize, cfg: Option<DBRevConfig>) -> Option<Revision> {
        let mut inner = self.inner.lock();
        let rlen = inner.revisions.len();
        if nback == 0 || nback > inner.max_revisions {
            return None
        }
        if rlen < nback {
            let ashes = inner.disk_requester.collect_ash(nback);
            for mut ash in ashes.into_iter().skip(rlen) {
                for (_, a) in ash.0.iter_mut() {
                    a.old.reverse()
                }

                let u = match inner.revisions.back() {
                    Some(u) => u.to_mem_store_r(),
                    None => inner.cached.to_mem_store_r(),
                };
                inner.revisions.push_back(u.rewind(
                    &ash.0[&MERKLE_META_SPACE].old,
                    &ash.0[&MERKLE_COMPACT_SPACE].old,
                    &ash.0[&BLOB_META_SPACE].old,
                    &ash.0[&BLOB_COMPACT_SPACE].old,
                ));
            }
        }
        if inner.revisions.len() < nback {
            return None
        }
        // set up the storage layout
        let merkle_payload_header: ObjPtr<CompactSpaceHeader>;
        let merkle_header: ObjPtr<MerkleHeader>;
        let blob_payload_header: ObjPtr<CompactSpaceHeader>;
        unsafe {
            let mut offset = std::mem::size_of::<DBHeader>() as u64;
            merkle_payload_header = ObjPtr::new_from_addr(offset);
            offset += CompactSpaceHeader::MSIZE;
            merkle_header = ObjPtr::new_from_addr(offset);
            blob_payload_header = ObjPtr::new_from_addr(0);
        }

        let space = &inner.revisions[nback - 1];

        let (merkle_payload_header_ref, merkle_header_ref, blob_payload_header_ref) = unsafe {
            let merkle_meta_ref = &space.merkle.meta as &dyn MemStore;
            let blob_meta_ref = &space.blob.meta as &dyn MemStore;

            (
                MummyObj::ptr_to_obj(
                    merkle_meta_ref,
                    merkle_payload_header,
                    shale::compact::CompactHeader::MSIZE,
                )
                .unwrap(),
                MummyObj::ptr_to_obj(merkle_meta_ref, merkle_header, MerkleHeader::MSIZE).unwrap(),
                MummyObj::ptr_to_obj(blob_meta_ref, blob_payload_header, shale::compact::CompactHeader::MSIZE).unwrap(),
            )
        };

        let merkle_space = shale::compact::CompactSpace::new(
            Rc::new(space.merkle.meta.clone()),
            Rc::new(space.merkle.payload.clone()),
            merkle_payload_header_ref,
            shale::ObjCache::new(cfg.as_ref().unwrap_or(&self.rev_cfg).merkle_ncached_objs),
            0,
            self.compact_regn_nbit,
        )
        .unwrap();

        let blob_space = shale::compact::CompactSpace::new(
            Rc::new(space.blob.meta.clone()),
            Rc::new(space.blob.payload.clone()),
            blob_payload_header_ref,
            shale::ObjCache::new(cfg.as_ref().unwrap_or(&self.rev_cfg).blob_ncached_objs),
            0,
            self.compact_regn_nbit,
        )
        .unwrap();

        Some(Revision {
            _m: inner,
            rev: DBRev {
                header: merkle_header_ref,
                merkle: Merkle::new(Box::new(merkle_space)),
                blob: BlobStash::new(Box::new(blob_space)),
            },
        })
    }
}

pub struct Revision<'a> {
    _m: MutexGuard<'a, DBInner>,
    rev: DBRev,
}

impl<'a> std::ops::Deref for Revision<'a> {
    type Target = DBRev;
    fn deref(&self) -> &DBRev {
        &self.rev
    }
}

pub struct WriteBatch<'a> {
    m: MutexGuard<'a, DBInner>,
    committed: bool,
}

impl<'a> WriteBatch<'a> {
    pub fn kv_insert<K: AsRef<[u8]>>(&mut self, key: K, val: Vec<u8>) -> Result<(), DBError> {
        let (header, merkle, _) = self.m.latest.borrow_split();
        merkle.insert(key, val, header.kv_root).map_err(DBError::Merkle)
    }

    pub fn kv_remove<K: AsRef<[u8]>>(&mut self, key: K) -> Result<bool, DBError> {
        let (header, merkle, _) = self.m.latest.borrow_split();
        merkle.remove(key, header.kv_root).map_err(DBError::Merkle)
    }

    pub fn kv_root_hash(&self) -> Result<Hash, DBError> {
        self.m
            .latest
            .merkle
            .root_hash::<IdTrans>(self.m.latest.header.kv_root)
            .map_err(DBError::Merkle)
    }

    pub fn root_hash(&self) -> Result<Hash, DBError> {
        self.m
            .latest
            .merkle
            .root_hash::<AccountRLP>(self.m.latest.header.acc_root)
            .map_err(DBError::Merkle)
    }

    fn change_account(
        &mut self, key: &[u8], modify: impl FnOnce(&mut Account, &mut BlobStash) -> Result<(), DBError>,
    ) -> Result<(), DBError> {
        let (header, merkle, blob) = self.m.latest.borrow_split();
        match merkle.get_mut(key, header.acc_root) {
            Ok(mut bytes) => {
                let mut ret = Ok(());
                bytes
                    .write(|b| {
                        let mut acc = Account::deserialize(b);
                        ret = modify(&mut acc, blob);
                        if ret.is_err() {
                            return
                        }
                        *b = acc.serialize();
                    })
                    .map_err(DBError::Merkle)?;
                ret?;
            }
            Err(MerkleError::KeyNotFound) => {
                let mut acc = Account::default();
                modify(&mut acc, blob)?;
                merkle
                    .insert(key, acc.serialize(), header.acc_root)
                    .map_err(DBError::Merkle)?;
            }
            Err(e) => return Err(DBError::Merkle(e)),
        }
        Ok(())
    }

    pub fn set_balance(&mut self, key: &[u8], balance: U256) -> Result<(), DBError> {
        self.change_account(key, |acc, _| Ok(acc.balance = balance))
    }

    pub fn set_code(&mut self, key: &[u8], code: &[u8]) -> Result<(), DBError> {
        use sha3::Digest;
        self.change_account(key, |acc, blob_stash| {
            if !acc.code.is_null() {
                blob_stash.free_blob(acc.code).map_err(DBError::Blob)?;
            }
            Ok(acc.set_code(
                Hash(sha3::Keccak256::digest(code).into()),
                blob_stash
                    .new_blob(Blob::Code(code.to_vec()))
                    .map_err(DBError::Blob)?
                    .as_ptr(),
            ))
        })
    }

    pub fn set_nonce(&mut self, key: &[u8], nonce: u64) -> Result<(), DBError> {
        self.change_account(key, |acc, _| Ok(acc.nonce = nonce))
    }

    pub fn set_state(&mut self, key: &[u8], sub_key: &[u8], val: Vec<u8>) -> Result<(), DBError> {
        let (header, merkle, _) = self.m.latest.borrow_split();
        let mut acc = match merkle.get(key, header.acc_root) {
            Ok(r) => Account::deserialize(&*r),
            Err(MerkleError::KeyNotFound) => Account::default(),
            Err(e) => return Err(DBError::Merkle(e)),
        };
        if acc.root.is_null() {
            Merkle::init_root(&mut acc.root, merkle.get_store()).map_err(DBError::Merkle)?;
        }
        merkle.insert(sub_key, val, acc.root).map_err(DBError::Merkle)?;
        acc.root_hash = merkle.root_hash::<IdTrans>(acc.root).map_err(DBError::Merkle)?;
        merkle
            .insert(key, acc.serialize(), header.acc_root)
            .map_err(DBError::Merkle)
    }

    pub fn create_account(&mut self, key: &[u8]) -> Result<(), DBError> {
        let (header, merkle, _) = self.m.latest.borrow_split();
        let old_balance = match merkle.get_mut(key, header.acc_root) {
            Ok(bytes) => Account::deserialize(&*bytes.get()).balance,
            Err(MerkleError::KeyNotFound) => U256::zero(),
            Err(e) => return Err(DBError::Merkle(e)),
        };
        let mut acc = Account::default();
        acc.balance = old_balance;
        merkle
            .insert(key, acc.serialize(), header.acc_root)
            .map_err(DBError::Merkle)
    }

    pub fn commit(mut self) {
        use crate::storage::BufferWrite;
        let inner = &mut *self.m;
        // clear the staging layer and apply changes to the CachedSpace
        let (merkle_payload_pages, merkle_payload_plain) = inner.staging.merkle.payload.take_delta();
        let (merkle_meta_pages, merkle_meta_plain) = inner.staging.merkle.meta.take_delta();
        let (blob_payload_pages, blob_payload_plain) = inner.staging.blob.payload.take_delta();
        let (blob_meta_pages, blob_meta_plain) = inner.staging.blob.meta.take_delta();

        let old_merkle_meta_delta = inner.cached.merkle.meta.update(&merkle_meta_pages).unwrap();
        let old_merkle_payload_delta = inner.cached.merkle.payload.update(&merkle_payload_pages).unwrap();
        let old_blob_meta_delta = inner.cached.blob.meta.update(&blob_meta_pages).unwrap();
        let old_blob_payload_delta = inner.cached.blob.payload.update(&blob_payload_pages).unwrap();

        // update the rolling window of past revisions
        let new_base = Universe {
            merkle: SubSpace::new(
                StoreRevShared::from_delta(inner.cached.merkle.meta.clone(), old_merkle_meta_delta),
                StoreRevShared::from_delta(inner.cached.merkle.payload.clone(), old_merkle_payload_delta),
            ),
            blob: SubSpace::new(
                StoreRevShared::from_delta(inner.cached.blob.meta.clone(), old_blob_meta_delta),
                StoreRevShared::from_delta(inner.cached.blob.payload.clone(), old_blob_payload_delta),
            ),
        };

        if let Some(rev) = inner.revisions.front_mut() {
            rev.merkle.meta.set_prev(new_base.merkle.meta.inner().clone());
            rev.merkle.payload.set_prev(new_base.merkle.payload.inner().clone());
            rev.blob.meta.set_prev(new_base.blob.meta.inner().clone());
            rev.blob.payload.set_prev(new_base.blob.payload.inner().clone());
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
                    space_id: inner.staging.merkle.payload.id(),
                    delta: merkle_payload_pages,
                },
                BufferWrite {
                    space_id: inner.staging.merkle.meta.id(),
                    delta: merkle_meta_pages,
                },
                BufferWrite {
                    space_id: inner.staging.blob.payload.id(),
                    delta: blob_payload_pages,
                },
                BufferWrite {
                    space_id: inner.staging.blob.meta.id(),
                    delta: blob_meta_pages,
                },
            ],
            crate::storage::AshRecord(
                [
                    (MERKLE_META_SPACE, merkle_meta_plain),
                    (MERKLE_COMPACT_SPACE, merkle_payload_plain),
                    (BLOB_META_SPACE, blob_meta_plain),
                    (BLOB_COMPACT_SPACE, blob_payload_plain),
                ]
                .into(),
            ),
        );
    }
}

impl<'a> Drop for WriteBatch<'a> {
    fn drop(&mut self) {
        if !self.committed {
            // drop the staging changes
            self.m.staging.merkle.payload.take_delta();
            self.m.staging.merkle.meta.take_delta();
            self.m.staging.blob.payload.take_delta();
            self.m.staging.blob.meta.take_delta();
        }
    }
}
