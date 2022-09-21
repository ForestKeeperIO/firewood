use std::cell::{RefCell, RefMut};
use std::collections::HashMap;
use std::fmt;
use std::num::NonZeroUsize;
use std::rc::Rc;
use std::sync::Arc;

use aiofut::{AIOBuilder, AIOManager};
use nix::fcntl::{flock, FlockArg};
use shale::{MemStore, MemView, SpaceID};
use tokio::sync::{mpsc, oneshot};
use typed_builder::TypedBuilder;

use crate::file::{Fd, File};

pub(crate) const PAGE_SIZE_NBIT: u64 = 12;
pub(crate) const PAGE_SIZE: u64 = 1 << PAGE_SIZE_NBIT;
pub(crate) const PAGE_MASK: u64 = PAGE_SIZE - 1;

pub trait MemStoreR {
    fn get_slice(&self, offset: u64, length: u64) -> Option<Vec<u8>>;
    fn id(&self) -> SpaceID;
}

type Page = [u8; PAGE_SIZE as usize];

pub struct SpaceWrite {
    offset: u64,
    data: Box<[u8]>,
}

/// Basic copy-on-write item in the linear storage space for multi-versioning.
pub struct DeltaPage(u64, Page);

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
pub struct StoreDelta(Vec<DeltaPage>);

impl fmt::Debug for StoreDelta {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "<StoreDelta>")
    }
}

impl std::ops::Deref for StoreDelta {
    type Target = [DeltaPage];
    fn deref(&self) -> &[DeltaPage] {
        &self.0
    }
}

impl StoreDelta {
    pub fn new(src: &dyn MemStoreR, writes: &[SpaceWrite]) -> Option<Self> {
        let mut deltas = Vec::new();
        let mut widx: Vec<_> = (0..writes.len()).filter(|i| writes[*i].data.len() > 0).collect();
        if widx.is_empty() {
            // the writes are all empty
            return None
        }

        // sort by the starting point
        widx.sort_by_key(|i| writes[*i].offset);

        let mut witer = widx.into_iter();
        let w0 = &writes[witer.next().unwrap()];
        let mut head = w0.offset >> PAGE_SIZE_NBIT;
        let mut tail = (w0.offset + w0.data.len() as u64 - 1) >> PAGE_SIZE_NBIT;

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
            let ep = (w.offset + w.data.len() as u64 - 1) >> PAGE_SIZE_NBIT;
            let wp = w.offset >> PAGE_SIZE_NBIT;
            if wp > tail {
                // all following writes won't go back past w.offset, so the previous continous
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
                (*if w.offset < deltas[mid].offset() {
                    &mut r
                } else {
                    &mut l
                }) = mid;
            }
            let off = (w.offset - deltas[l].offset()) as usize;
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

    pub fn len(&self) -> usize {
        self.0.len()
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
    pub fn apply_change(prev: Arc<dyn MemStoreR>, writes: &[SpaceWrite]) -> Option<StoreRevShared> {
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
    fn mem_image(&self) -> &dyn MemStore {
        &self.store
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

    pub fn to_delta(&self) -> StoreDelta {
        let mut pages = Vec::new();
        for (pid, page) in self.deltas.borrow().iter() {
            pages.push(DeltaPage(*pid, **page));
        }
        StoreDelta(pages)
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
            writes.push(SpaceWrite { offset: l, data });
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
    disk_buffer: DiskBufferRequester,
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
                disk_buffer: DiskBufferRequester::default(),
            })),
            space_id,
        })
    }
}

impl CachedSpaceInner {
    fn fetch_page(&mut self, space_id: SpaceID, pid: u64) -> Result<Box<Page>, StoreError> {
        if let Some(p) = self.disk_buffer.get_page(space_id, pid) {
            return Ok(Box::new((*p).clone()))
        }
        let file_nbit = self.files.get_file_nbit();
        let file_size = 1 << file_nbit;
        let poff = pid << PAGE_SIZE_NBIT;
        let file = self.files.get_file(poff >> file_nbit)?;
        let mut page: Page = [0; PAGE_SIZE as usize];
        nix::sys::uio::pread(file.get_fd(), &mut page, (poff & (file_size - 1)) as nix::libc::off_t)
            .map_err(StoreError::System)?;
        Ok(Box::new(page))
    }

    fn pin_page(&mut self, space_id: SpaceID, pid: u64) -> Result<&'static [u8], StoreError> {
        let base = match self.pinned_pages.get_mut(&pid) {
            Some(mut e) => {
                e.0 += 1;
                e.1.as_ptr()
            }
            None => {
                let page = match self.cached_pages.pop(&pid) {
                    Some(p) => p,
                    None => self.fetch_page(space_id, pid)?,
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
            data: store.inner.borrow_mut().pin_page(store.space_id, pid).ok()?,
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

struct FilePool {
    files: RefCell<lru::LruCache<u64, Rc<File>>>,
    file_nbit: u64,
    rootfd: Fd,
}

impl FilePool {
    fn new(cfg: &StoreConfig) -> Result<Self, StoreError> {
        let rootfd = cfg.rootfd;
        let file_nbit = cfg.file_nbit;
        let s = Self {
            files: RefCell::new(lru::LruCache::new(
                NonZeroUsize::new(cfg.ncached_files).expect("non-zero file num"),
            )),
            file_nbit,
            rootfd,
        };
        let f0 = s.get_file(0)?;
        if let Err(_) = flock(f0.get_fd(), FlockArg::LockExclusiveNonblock) {
            return Err(StoreError::InitError("the store is busy".into()))
        }
        Ok(s)
    }

    fn get_file(&self, fid: u64) -> Result<Rc<File>, StoreError> {
        let mut files = self.files.borrow_mut();
        let file_size = 1 << self.file_nbit;
        Ok(match files.get(&fid) {
            Some(f) => f.clone(),
            None => {
                files.put(
                    fid,
                    Rc::new(File::new(fid, file_size, self.rootfd).map_err(StoreError::System)?),
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
        let f0 = self.get_file(0).unwrap();
        flock(f0.get_fd(), FlockArg::UnlockNonblock).ok();
        nix::unistd::close(self.rootfd).ok();
    }
}

#[derive(Debug)]
pub struct BufferWrite {
    pub space_id: SpaceID,
    pub delta: StoreDelta,
}

#[derive(Debug)]
enum BufferCmd {
    GetPage((SpaceID, u64), oneshot::Sender<Option<Arc<Page>>>),
    WriteBatch(Vec<BufferWrite>),
    Shutdown,
}

#[derive(TypedBuilder)]
pub struct DiskBufferConfig {
    #[builder(default = 4096)]
    max_buffered: usize,
    #[builder(default = 4096)]
    max_pending: usize,
    #[builder(default = 128)]
    max_aio_requests: u32,
    #[builder(default = 128)]
    max_aio_response: u16,
    #[builder(default = 128)]
    max_aio_submit: usize,
}

struct PendingPage {
    data: Arc<Page>,
    file_nbit: u64,
    updated: bool,
}

pub struct DiskBuffer {
    pending: HashMap<(SpaceID, u64), PendingPage>,
    inbound: mpsc::Receiver<BufferCmd>,
    requester: DiskBufferRequester,
    fc_notifier: Option<oneshot::Sender<()>>,
    fc_blocker: Option<oneshot::Receiver<()>>,
    file_pools: [Option<Rc<FilePool>>; 255],
    aiomgr: AIOManager,
    max_pending: usize,
    local_pool: Rc<tokio::task::LocalSet>,
}

impl DiskBuffer {
    pub fn new(cfg: &DiskBufferConfig) -> Option<Self> {
        const INIT: Option<Rc<FilePool>> = None;

        let (sender, inbound) = mpsc::channel(cfg.max_buffered);
        let requester = DiskBufferRequester::new(sender);
        let max_pending = cfg.max_pending;
        let aiomgr = AIOBuilder::default()
            .max_events(cfg.max_aio_requests)
            .max_nwait(cfg.max_aio_response)
            .max_nbatched(cfg.max_aio_submit)
            .build()
            .ok()?;

        Some(Self {
            pending: HashMap::new(),
            max_pending,
            inbound,
            requester,
            fc_notifier: None,
            fc_blocker: None,
            file_pools: [INIT; 255],
            aiomgr,
            local_pool: Rc::new(tokio::task::LocalSet::new()),
        })
    }

    pub fn reg_cached_space(&mut self, space: &CachedSpace) {
        let mut inner = space.inner.borrow_mut();
        inner.disk_buffer = self.requester().clone();
        self.file_pools[space.id() as usize] = Some(inner.files.clone())
    }

    unsafe fn get_longlive_self(&mut self) -> &'static mut Self {
        std::mem::transmute::<&mut Self, &'static mut Self>(self)
    }

    fn schedule_write(&mut self, page_key: (SpaceID, u64)) {
        let p = self.pending.get(&page_key).unwrap();
        let offset = page_key.1 << PAGE_SIZE_NBIT;
        let fid = offset >> p.file_nbit;
        let fmask = (1 << p.file_nbit) - 1;
        let file = self.file_pools[page_key.0 as usize]
            .as_ref()
            .unwrap()
            .get_file(fid)
            .unwrap();
        let fut = self
            .aiomgr
            .write(file.get_fd(), offset & fmask, Box::new(*p.data), None);
        let s = unsafe { self.get_longlive_self() };
        self.local_pool.spawn_local(async move {
            let (res, _) = fut.await;
            res.unwrap();
            s.finish_write(page_key);
        });
    }

    fn finish_write(&mut self, page_key: (SpaceID, u64)) {
        use std::collections::hash_map::Entry::*;
        match self.pending.entry(page_key) {
            Occupied(e) => {
                if e.get().updated {
                    // write again
                    self.schedule_write(page_key);
                }
            }
            _ => unreachable!(),
        }
    }

    async fn process(&mut self, req: BufferCmd) -> bool {
        use std::collections::hash_map::Entry::*;
        match req {
            BufferCmd::Shutdown => return false,
            BufferCmd::GetPage(page_key, tx) => tx.send(self.pending.get(&page_key).map(|e| e.data.clone())).unwrap(),
            BufferCmd::WriteBatch(writes) => {
                for BufferWrite { space_id, delta } in writes {
                    for w in delta.0 {
                        let page_key = (space_id, w.0);
                        match self.pending.entry(page_key) {
                            Occupied(mut e) => {
                                let e = e.get_mut();
                                e.data = Arc::new(w.1);
                                e.updated = true;
                            }
                            Vacant(e) => {
                                let file_nbit = self.file_pools[page_key.0 as usize].as_ref().unwrap().file_nbit;
                                e.insert(PendingPage {
                                    data: Arc::new(w.1),
                                    file_nbit,
                                    updated: false,
                                });
                                self.schedule_write(page_key);
                            }
                        }
                    }
                }
                if self.pending.len() >= self.max_pending {
                    let (tx, rx) = oneshot::channel();
                    self.fc_notifier = Some(tx);
                    self.fc_blocker = Some(rx);
                }
            }
        }
        true
    }

    #[tokio::main(flavor = "current_thread")]
    pub async fn run(mut self) {
        std::sync::atomic::fence(std::sync::atomic::Ordering::SeqCst);
        self.local_pool
            .clone()
            .run_until(async {
                loop {
                    if let Some(fc) = self.fc_blocker.take() {
                        // flow control, wait until ready
                        fc.await.unwrap();
                    }
                    let req = self.inbound.recv().await.unwrap();
                    if !self.process(req).await {
                        break
                    }
                }
            })
            .await;
    }

    pub fn requester(&self) -> &DiskBufferRequester {
        &self.requester
    }
}

unsafe impl Send for DiskBuffer {}

#[derive(Clone)]
pub struct DiskBufferRequester {
    sender: mpsc::Sender<BufferCmd>,
}

impl Default for DiskBufferRequester {
    fn default() -> Self {
        Self {
            sender: mpsc::channel(1).0,
        }
    }
}

impl DiskBufferRequester {
    fn new(sender: mpsc::Sender<BufferCmd>) -> Self {
        Self { sender }
    }

    pub fn get_page(&self, space_id: SpaceID, pid: u64) -> Option<Arc<Page>> {
        let (resp_tx, resp_rx) = oneshot::channel();
        self.sender
            .blocking_send(BufferCmd::GetPage((space_id, pid), resp_tx))
            .unwrap();
        resp_rx.blocking_recv().unwrap()
    }

    pub fn write(&self, batch: Vec<BufferWrite>) {
        self.sender.blocking_send(BufferCmd::WriteBatch(batch)).unwrap()
    }

    pub fn shutdown(&self) {
        self.sender.blocking_send(BufferCmd::Shutdown).unwrap()
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
