use std::cell::{RefCell, RefMut};
use std::collections::HashMap;
use std::fmt;
use std::num::NonZeroUsize;
use std::rc::Rc;
use std::sync::Arc;

use nix::fcntl::{flock, FlockArg};
use shale::{DiskWrite, MemStore, MemView, SpaceID};
use typed_builder::TypedBuilder;

use crate::file::{Fd, File};

const PAGE_SIZE_NBIT: u64 = 12;
const PAGE_SIZE: u64 = 1 << PAGE_SIZE_NBIT;
const PAGE_MASK: u64 = PAGE_SIZE - 1;

pub trait MemStoreR {
    fn get_slice(&self, offset: u64, length: u64) -> Option<Vec<u8>>;
    fn id(&self) -> SpaceID;
}

type Page = [u8; PAGE_SIZE as usize];

/// Basic copy-on-write item in the linear storage space for multi-versioning.
struct DeltaPage(u64, Page);

impl DeltaPage {
    #[inline(always)]
    fn offset(&self) -> u64 {
        self.0 << PAGE_SIZE_NBIT
    }

    #[inline(always)]
    fn data(&self) -> &[u8] {
        &self.1
    }

    #[inline(always)]
    fn data_mut(&mut self) -> &mut [u8] {
        &mut self.1
    }
}

#[derive(Default)]
struct StoreDelta(Vec<DeltaPage>);

impl std::ops::Deref for StoreDelta {
    type Target = [DeltaPage];
    fn deref(&self) -> &[DeltaPage] {
        &self.0
    }
}

impl StoreDelta {
    pub fn new(src: &dyn MemStoreR, writes: &[DiskWrite]) -> Option<Self> {
        let mut deltas = Vec::new();
        let mut widx: Vec<_> = (0..writes.len()).filter(|i| writes[*i].data.len() > 0).collect();
        if widx.is_empty() {
            // the writes are all empty
            return None
        }

        // sort by the starting point
        widx.sort_by_key(|i| writes[*i].space_off);

        let mut witer = widx.into_iter();
        let w0 = &writes[witer.next().unwrap()];
        let mut head = w0.space_off >> PAGE_SIZE_NBIT;
        let mut tail = (w0.space_off + w0.data.len() as u64 - 1) >> PAGE_SIZE_NBIT;

        macro_rules! create_dirty_pages {
            ($l: expr, $r: expr) => {
                for p in $l..=$r {
                    let off = p << PAGE_SIZE_NBIT;
                    deltas.push(DeltaPage(
                        p,
                        src.get_slice(off, PAGE_SIZE).unwrap().try_into().unwrap(),
                    ));
                }
            };
        }

        for i in witer {
            let w = &writes[i];
            let ep = (w.space_off + w.data.len() as u64 - 1) >> PAGE_SIZE_NBIT;
            let wp = w.space_off >> PAGE_SIZE_NBIT;
            if wp > tail {
                // all following writes won't go back past w.space_off, so the previous continous
                // write area is determined
                create_dirty_pages!(head, tail);
                head = wp;
            }
            tail = std::cmp::max(tail, ep)
        }
        create_dirty_pages!(head, tail);

        let psize = PAGE_SIZE as usize;
        for w in writes.into_iter() {
            let mut l = 0;
            let mut r = deltas.len();
            while r - l > 1 {
                let mid = (l + r) >> 1;
                (*if w.space_off < deltas[mid].offset() {
                    &mut r
                } else {
                    &mut l
                }) = mid;
            }
            let off = (w.space_off - deltas[l].offset()) as usize;
            let len = std::cmp::min(psize - off, w.data.len());
            deltas[l].data_mut()[off..off + len].copy_from_slice(&w.data[..len]);
            let mut data = &w.data[len..];
            while data.len() >= psize {
                l += 1;
                deltas[l].data_mut().copy_from_slice(&data[..psize]);
                data = &data[psize..];
            }
            if data.len() > 0 {
                l += 1;
                deltas[l].data_mut()[..data.len()].copy_from_slice(&data);
            }
        }
        Some(Self(deltas))
    }
}

struct StoreRev {
    prev: Arc<dyn MemStoreR>,
    deltas: StoreDelta,
}

impl fmt::Debug for StoreRev {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "<StoreRev")?;
        for d in self.deltas.iter() {
            write!(f, " 0x{:x}", d.0)?;
        }
        write!(f, ">\n")
    }
}

impl MemStoreR for StoreRev {
    fn get_slice(&self, offset: u64, length: u64) -> Option<Vec<u8>> {
        let mut start = offset;
        let end = start + length;
        let deltas = &self.deltas;
        let mut l = 0;
        let mut r = deltas.len();
        // no dirty page, before or after all dirty pages
        if r == 0 {
            return self.prev.get_slice(start, end - start)
        }
        // otherwise, some dirty pages are covered by the range
        while r - l > 1 {
            let mid = (l + r) >> 1;
            (*if start < deltas[mid].offset() { &mut r } else { &mut l }) = mid;
        }
        if start >= deltas[l].offset() + PAGE_SIZE {
            l += 1
        }
        if l >= deltas.len() || end < deltas[l].offset() {
            return self.prev.get_slice(start, end - start)
        }
        let mut data = Vec::new();
        let p_off = std::cmp::min(end - deltas[l].offset(), PAGE_SIZE);
        if start < deltas[l].offset() {
            data.extend(self.prev.get_slice(start, deltas[l].offset() - start)?);
            data.extend(&deltas[l].data()[..p_off as usize]);
        } else {
            data.extend(&deltas[l].data()[(start - deltas[l].offset()) as usize..p_off as usize]);
        };
        start = deltas[l].offset() + p_off;
        while start < end {
            l += 1;
            if l >= deltas.len() || end < deltas[l].offset() {
                data.extend(self.prev.get_slice(start, end - start)?);
                break
            }
            if deltas[l].offset() > start {
                data.extend(self.prev.get_slice(start, deltas[l].offset() - start)?);
            }
            if end < deltas[l].offset() + PAGE_SIZE {
                data.extend(&deltas[l].data()[..(end - deltas[l].offset()) as usize]);
                break
            }
            data.extend(deltas[l].data());
            start = deltas[l].offset() + PAGE_SIZE;
        }
        assert!(data.len() == length as usize);
        Some(data)
    }

    fn id(&self) -> SpaceID {
        self.prev.id()
    }
}

#[derive(Clone, Debug)]
pub struct StoreRevShared(Arc<StoreRev>);

impl StoreRevShared {
    pub fn apply_change(prev: Arc<dyn MemStoreR>, writes: &[DiskWrite]) -> Option<StoreRevShared> {
        let deltas = StoreDelta::new(prev.as_ref(), writes)?;
        Some(Self(Arc::new(StoreRev { prev, deltas })))
    }
}

struct StoreRef<S: Clone + MemStore> {
    data: Vec<u8>,
    store: S,
}

impl<S: Clone + MemStore> std::ops::Deref for StoreRef<S> {
    type Target = [u8];
    fn deref(&self) -> &[u8] {
        &self.data
    }
}

impl<S: Clone + MemStore + 'static> MemView for StoreRef<S> {
    fn mem_image(&self) -> Box<dyn MemStore> {
        Box::new(self.store.clone())
    }
}

impl MemStore for StoreRevShared {
    fn get_ref(&self, offset: u64, length: u64) -> Option<Box<dyn MemView>> {
        let store = self.clone();
        let data = self.0.get_slice(offset, length)?;
        Some(Box::new(StoreRef { data, store }))
    }

    fn write(&self, _offset: u64, _change: &[u8]) {
        // StoreRevShared is a read-only view version of MemStore
        unimplemented!()
    }

    fn id(&self) -> SpaceID {
        <StoreRev as MemStoreR>::id(&self.0)
    }
}

#[derive(Clone)]
pub struct StoreRevMut {
    prev: Arc<dyn MemStoreR>,
    deltas: Rc<RefCell<HashMap<u64, Box<Page>>>>,
}

impl StoreRevMut {
    pub fn new(prev: Arc<dyn MemStoreR>) -> Self {
        Self {
            prev,
            deltas: Rc::new(RefCell::new(HashMap::new())),
        }
    }

    fn get_page_mut(&self, pid: u64) -> RefMut<[u8]> {
        let mut deltas = self.deltas.borrow_mut();
        if deltas.get(&pid).is_none() {
            let page = Box::new(
                self.prev
                    .get_slice(pid << PAGE_SIZE_NBIT, PAGE_SIZE)
                    .unwrap()
                    .try_into()
                    .unwrap(),
            );
            deltas.insert(pid, page);
        }
        RefMut::map(deltas, |e| &mut e.get_mut(&pid).unwrap()[..])
    }
}

impl MemStore for StoreRevMut {
    fn get_ref(&self, offset: u64, length: u64) -> Option<Box<dyn MemView>> {
        let data = if length == 0 {
            Vec::new()
        } else {
            let end = offset + length - 1;
            let s_pid = offset >> PAGE_SIZE_NBIT;
            let s_off = (offset & PAGE_MASK) as usize;
            let e_pid = end >> PAGE_SIZE_NBIT;
            let e_off = (end & PAGE_MASK) as usize;
            let deltas = self.deltas.borrow();
            if s_pid == e_pid {
                match deltas.get(&s_pid) {
                    Some(p) => p[s_off..e_off + 1].to_vec(),
                    None => self.prev.get_slice(offset, length)?,
                }
            } else {
                let mut data = match deltas.get(&s_pid) {
                    Some(p) => p[s_off..].to_vec(),
                    None => self.prev.get_slice(offset, PAGE_SIZE - s_off as u64)?,
                };
                for p in s_pid + 1..e_pid {
                    match deltas.get(&p) {
                        Some(p) => data.extend(&**p),
                        None => data.extend(&self.prev.get_slice(p << PAGE_SIZE_NBIT, PAGE_SIZE)?),
                    };
                }
                match deltas.get(&e_pid) {
                    Some(p) => data.extend(&p[..e_off + 1]),
                    None => data.extend(self.prev.get_slice(e_pid << PAGE_SIZE_NBIT, e_off as u64 + 1)?),
                }
                data
            }
        };
        let store = self.clone();
        Some(Box::new(StoreRef { data, store }))
    }

    fn write(&self, offset: u64, mut change: &[u8]) {
        let length = change.len() as u64;
        let end = offset + length - 1;
        let s_pid = offset >> PAGE_SIZE_NBIT;
        let s_off = (offset & PAGE_MASK) as usize;
        let e_pid = end >> PAGE_SIZE_NBIT;
        let e_off = (end & PAGE_MASK) as usize;
        if s_pid == e_pid {
            self.get_page_mut(s_pid)[s_off..e_off + 1].copy_from_slice(change)
        } else {
            let len = PAGE_SIZE as usize - s_off;
            self.get_page_mut(s_pid)[s_off..].copy_from_slice(&change[..len]);
            change = &change[..len];
            for p in s_pid + 1..e_pid {
                self.get_page_mut(p).copy_from_slice(&change[..PAGE_SIZE as usize]);
                change = &change[..PAGE_SIZE as usize];
            }
            self.get_page_mut(e_pid)[..e_off + 1].copy_from_slice(change);
        }
    }

    fn id(&self) -> SpaceID {
        self.prev.id()
    }
}

#[derive(Clone)]
pub struct ZeroStore(Arc<()>);

impl ZeroStore {
    pub fn new() -> Self {
        Self(Arc::new(()))
    }
}

impl MemStoreR for ZeroStore {
    fn get_slice(&self, _: u64, length: u64) -> Option<Vec<u8>> {
        Some(vec![0; length as usize])
    }

    fn id(&self) -> SpaceID {
        shale::INVALID_SPACE_ID
    }
}

#[test]
fn test_apply_change() {
    use rand::{rngs::StdRng, Rng, SeedableRng};
    let mut rng = StdRng::seed_from_u64(42);
    let min = rng.gen_range(0..2 * PAGE_SIZE);
    let max = rng.gen_range(min + 1 * PAGE_SIZE..min + 100 * PAGE_SIZE);
    for _ in 0..2000 {
        let n = 20;
        let mut canvas = Vec::new();
        canvas.resize((max - min) as usize, 0);
        let mut writes: Vec<_> = Vec::new();
        for _ in 0..n {
            let l = rng.gen_range(min..max);
            let r = rng.gen_range(l + 1..std::cmp::min(l + 3 * PAGE_SIZE, max));
            let data: Box<[u8]> = (l..r).map(|_| rng.gen()).collect();
            for (idx, byte) in (l..r).zip(data.iter()) {
                canvas[(idx - min) as usize] = *byte;
            }
            println!("[0x{:x}, 0x{:x})", l, r);
            writes.push(DiskWrite {
                space_id: 0x0,
                space_off: l,
                data,
            });
        }
        let z = Arc::new(ZeroStore::new());
        let rev = StoreRevShared::apply_change(z, &writes).unwrap();
        println!("{:?}", rev);
        assert_eq!(rev.get_ref(min, max - min).unwrap().deref(), &canvas);
        for _ in 0..2 * n {
            let l = rng.gen_range(min..max);
            let r = rng.gen_range(l + 1..max);
            assert_eq!(
                rev.get_ref(l, r - l).unwrap().deref(),
                &canvas[(l - min) as usize..(r - min) as usize]
            );
        }
    }
}

#[derive(TypedBuilder)]
pub struct StoreConfig {
    ncached_pages: usize,
    ncached_files: usize,
    #[builder(default = 22)] // 4MB file by default
    file_nbit: u64,
    space_id: SpaceID,
    rootfd: Fd,
}

struct CachedSpaceInner {
    cached_pages: lru::LruCache<u64, Box<Page>>,
    pinned_pages: HashMap<u64, (usize, Box<Page>)>,
    files: Rc<FilePool>,
    disk_buffer: Rc<DiskBuffer>,
}

#[derive(Clone)]
pub struct CachedSpace {
    inner: Rc<RefCell<CachedSpaceInner>>,
    space_id: SpaceID,
}

impl CachedSpace {
    pub fn new(cfg: &StoreConfig) -> Result<Self, StoreError> {
        let space_id = cfg.space_id;
        let files = Rc::new(FilePool::new(cfg)?);
        Ok(Self {
            inner: Rc::new(RefCell::new(CachedSpaceInner {
                cached_pages: lru::LruCache::new(NonZeroUsize::new(cfg.ncached_pages).expect("non-zero cache size")),
                pinned_pages: HashMap::new(),
                files,
                disk_buffer: Rc::new(DiskBuffer::new()),
            })),
            space_id,
        })
    }
}

impl CachedSpaceInner {
    fn fetch_page(&mut self, pid: u64) -> Result<Box<Page>, StoreError> {
        if let Some(p) = self.disk_buffer.get_page(pid) {
            return Ok(Box::new(p.clone()))
        }
        let file_nbit = self.files.get_file_nbit();
        let file_size = 1 << file_nbit;
        let file = self.files.get_file((pid << PAGE_SIZE_NBIT) >> file_nbit)?;
        let mut page: Page = [0; PAGE_SIZE as usize];
        nix::sys::uio::pread(file.get_fd(), &mut page, (pid & (1 - file_size)) as nix::libc::off_t)
            .map_err(StoreError::System)?;
        Ok(Box::new(page))
    }

    fn pin_page(&mut self, pid: u64) -> Result<&'static [u8], StoreError> {
        let base = match self.pinned_pages.get_mut(&pid) {
            Some(mut e) => {
                e.0 += 1;
                e.1.as_ptr()
            }
            None => {
                let page = match self.cached_pages.pop(&pid) {
                    Some(p) => p,
                    None => self.fetch_page(pid)?,
                };
                let ptr = page.as_ptr();
                self.pinned_pages.insert(pid, (1, page));
                ptr
            }
        };
        Ok(unsafe { std::slice::from_raw_parts(base, PAGE_SIZE as usize) })
    }

    fn unpin_page(&mut self, pid: u64) {
        use std::collections::hash_map::Entry::*;
        let page = match self.pinned_pages.entry(pid) {
            Occupied(mut e) => {
                let cnt = &mut e.get_mut().0;
                assert!(*cnt > 0);
                *cnt -= 1;
                if *cnt == 0 {
                    e.remove().1
                } else {
                    return
                }
            }
            _ => unreachable!(),
        };
        self.cached_pages.put(pid, page);
    }

    fn update_page(&mut self, pid: u64, data: &[u8]) {
        if let Some(v) = self.pinned_pages.get_mut(&pid) {
            v.1.copy_from_slice(data);
            return
        }
        if let Some(v) = self.cached_pages.peek_mut(&pid) {
            v.copy_from_slice(data);
        }
    }
}

struct PageRef {
    pid: u64,
    data: &'static [u8],
    store: CachedSpace,
}

impl<'a> std::ops::Deref for PageRef {
    type Target = [u8];
    fn deref(&self) -> &[u8] {
        self.data
    }
}

impl PageRef {
    fn new(pid: u64, store: &CachedSpace) -> Option<Self> {
        Some(Self {
            pid,
            data: store.inner.borrow_mut().pin_page(pid).ok()?,
            store: store.clone(),
        })
    }
}

impl Drop for PageRef {
    fn drop(&mut self) {
        self.store.inner.borrow_mut().unpin_page(self.pid);
    }
}

impl MemStoreR for CachedSpace {
    fn get_slice(&self, offset: u64, length: u64) -> Option<Vec<u8>> {
        if length == 0 {
            return Some(Default::default())
        }
        let end = offset + length - 1;
        let s_pid = offset >> PAGE_SIZE_NBIT;
        let s_off = (offset & PAGE_MASK) as usize;
        let e_pid = end >> PAGE_SIZE_NBIT;
        let e_off = (end & PAGE_MASK) as usize;
        if s_pid == e_pid {
            return PageRef::new(s_pid, self).map(|e| e[s_off..e_off + 1].to_vec())
        }
        let mut data: Vec<u8> = Vec::new();
        {
            data.extend(&PageRef::new(s_pid, self)?[s_off..]);
            for p in s_pid + 1..e_pid {
                data.extend(&PageRef::new(p, self)?[..]);
            }
            data.extend(&PageRef::new(e_pid, self)?[..e_off + 1]);
        }
        Some(data)
    }

    fn id(&self) -> SpaceID {
        self.space_id
    }
}

pub struct FilePool {
    files: RefCell<lru::LruCache<u64, Rc<File>>>,
    file_nbit: u64,
    rootfd: Fd,
}

impl FilePool {
    pub fn new(cfg: &StoreConfig) -> Result<Self, StoreError> {
        let rootfd = cfg.rootfd;
        let file_nbit = cfg.file_nbit;
        if let Err(_) = flock(rootfd, FlockArg::LockExclusiveNonblock) {
            return Err(StoreError::InitError("the store is busy".into()))
        }
        Ok(Self {
            files: RefCell::new(lru::LruCache::new(
                NonZeroUsize::new(cfg.ncached_files).expect("non-zero file num"),
            )),
            file_nbit,
            rootfd,
        })
    }

    fn get_file(&self, fid: u64) -> Result<Rc<File>, StoreError> {
        let mut files = self.files.borrow_mut();
        let file_size = 1 << self.file_nbit;
        Ok(match files.get(&fid) {
            Some(f) => f.clone(),
            None => {
                files.put(
                    fid,
                    Rc::new(File::new(fid, file_size, self.rootfd, false).map_err(StoreError::System)?),
                );
                files.peek(&fid).unwrap().clone()
            }
        })
    }

    fn get_file_nbit(&self) -> u64 {
        self.file_nbit
    }
}

impl Drop for FilePool {
    fn drop(&mut self) {
        flock(self.rootfd, FlockArg::UnlockNonblock).ok();
        nix::unistd::close(self.rootfd).ok();
    }
}

pub struct DiskBuffer {
    pending: HashMap<u64, Box<Page>>,
}

impl DiskBuffer {
    fn new() -> Self {
        Self {
            pending: HashMap::new(),
        }
    }

    fn get_page(&self, pid: u64) -> Option<&Page> {
        self.pending.get(&pid).map(|e| e.as_ref())
    }
}

pub struct DiskStore {
    disk_buffer: Rc<DiskBuffer>,
    file_pools: [Option<Rc<FilePool>>; 255],
}

impl DiskStore {
    pub fn new() -> Self {
        const INIT: Option<Rc<FilePool>> = None;
        Self {
            disk_buffer: Rc::new(DiskBuffer::new()),
            file_pools: [INIT; 255],
        }
    }

    pub fn reg_cached_space(&mut self, space: &CachedSpace) {
        let mut inner = space.inner.borrow_mut();
        inner.disk_buffer = self.disk_buffer.clone();
        self.file_pools[space.id() as usize] = Some(inner.files.clone())
    }
}

#[derive(Debug, PartialEq)]
pub enum StoreError {
    System(nix::Error),
    InitError(String),
    WriterError,
}

impl From<nix::Error> for StoreError {
    fn from(e: nix::Error) -> Self {
        StoreError::System(e)
    }
}
