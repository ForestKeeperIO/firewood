use std::cell::RefCell;
use std::collections::HashMap;
use std::fmt;
use std::rc::Rc;
use std::sync::Arc;

use nix::fcntl::{flock, FlockArg};
use shale::{DiskWrite, LinearRef, MemStore};
use typed_builder::TypedBuilder;

use crate::file::{Fd, File};

const PAGE_SIZE_NBIT: u64 = 12;
const PAGE_SIZE: u64 = 1 << PAGE_SIZE_NBIT;

pub enum StoreSource {
    Internal(StoreRev),
    External(Arc<dyn MemStore>),
}

impl StoreSource {
    fn as_mem_store(&self) -> &dyn MemStore {
        match self {
            Self::Internal(rev) => rev,
            Self::External(store) => store.as_ref(),
        }
    }
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

struct StoreDelta(Vec<DeltaPage>);

impl std::ops::Deref for StoreDelta {
    type Target = [DeltaPage];
    fn deref(&self) -> &[DeltaPage] {
        &self.0
    }
}

impl StoreDelta {
    pub fn new(src: &dyn MemStore, writes: &[DiskWrite]) -> Option<Self> {
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
                        src.get_ref(off, PAGE_SIZE).unwrap().deref().try_into().unwrap(),
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

struct StoreRevInner {
    prev: StoreSource,
    deltas: StoreDelta,
}

#[derive(Clone)]
pub struct StoreRev(Arc<StoreRevInner>);

impl fmt::Debug for StoreRev {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "<StoreRev")?;
        for d in self.0.deltas.iter() {
            write!(f, " 0x{:x}", d.0)?;
        }
        write!(f, ">\n")
    }
}

impl StoreRev {
    pub fn apply_change(prev: StoreSource, writes: &[DiskWrite]) -> Option<StoreRev> {
        let deltas = StoreDelta::new(
            match &prev {
                StoreSource::External(e) => e.as_ref(),
                StoreSource::Internal(i) => i,
            },
            writes,
        )?;
        Some(Self(Arc::new(StoreRevInner { prev, deltas })))
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

impl<S: Clone + MemStore + 'static> LinearRef for StoreRef<S> {
    fn mem_image(&self) -> Box<dyn MemStore> {
        Box::new(self.store.clone())
    }
}

impl MemStore for StoreRev {
    fn get_ref(&self, offset: u64, length: u64) -> Option<Box<dyn LinearRef>> {
        let mut start = offset;
        let end = start + length;
        let deltas = &self.0.deltas;
        let mut l = 0;
        let mut r = deltas.len();
        // no dirty page, before or after all dirty pages
        if r == 0 {
            return self.0.prev.as_mem_store().get_ref(start, end - start)
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
            return self.0.prev.as_mem_store().get_ref(start, end - start)
        }
        let mut data = Vec::new();
        let p_off = std::cmp::min(end - deltas[l].offset(), PAGE_SIZE);
        if start < deltas[l].offset() {
            data.extend(
                self.0
                    .prev
                    .as_mem_store()
                    .get_ref(start, deltas[l].offset() - start)?
                    .deref(),
            );
            data.extend(&deltas[l].data()[..p_off as usize]);
        } else {
            data.extend(&deltas[l].data()[(start - deltas[l].offset()) as usize..p_off as usize]);
        };
        start = deltas[l].offset() + p_off;
        while start < end {
            l += 1;
            if l >= deltas.len() || end < deltas[l].offset() {
                data.extend(self.0.prev.as_mem_store().get_ref(start, end - start)?.deref());
                break
            }
            if deltas[l].offset() > start {
                data.extend(
                    self.0
                        .prev
                        .as_mem_store()
                        .get_ref(start, deltas[l].offset() - start)?
                        .deref(),
                );
            }
            if end < deltas[l].offset() + PAGE_SIZE {
                data.extend(&deltas[l].data()[..(end - deltas[l].offset()) as usize]);
                break
            }
            data.extend(deltas[l].data());
            start = deltas[l].offset() + PAGE_SIZE;
        }
        assert!(data.len() == length as usize);

        let store = self.clone();
        Some(Box::new(StoreRef { data, store }))
    }

    fn write(&self, _offset: u64, _change: &[u8]) {
        panic!("StoreRev should not be written");
    }

    fn id(&self) -> shale::SpaceID {
        shale::INVALID_SPACE_ID
    }
}

#[derive(Clone)]
pub struct ZeroStore(Arc<()>);

impl ZeroStore {
    pub fn new() -> Self {
        Self(Arc::new(()))
    }
}

impl MemStore for ZeroStore {
    fn get_ref(&self, _: u64, length: u64) -> Option<Box<dyn LinearRef>> {
        Some(Box::new(StoreRef {
            data: vec![0; length as usize],
            store: self.clone(),
        }))
    }

    fn write(&self, _: u64, _: &[u8]) {
        panic!("ZeroStore should not be written");
    }

    fn id(&self) -> shale::SpaceID {
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
        let rev = StoreRev::apply_change(StoreSource::External(z), &writes).unwrap();
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
    rootfd: Fd,
    space_id: shale::SpaceID,
}

struct StoreInner {
    cached_pages: lru::LruCache<u64, Box<Page>>,
    pinned_pages: HashMap<u64, (usize, Box<Page>)>,
    cached_files: lru::LruCache<u64, File>,
    file_nbit: u64,
    rootfd: Fd,
}

#[derive(Clone)]
pub struct Store {
    inner: Rc<RefCell<StoreInner>>,
    space_id: shale::SpaceID,
}

impl Store {
    pub fn new(cfg: StoreConfig) -> Result<Self, StoreError> {
        use std::num::NonZeroUsize;
        if let Err(_) = flock(cfg.rootfd, FlockArg::LockExclusiveNonblock) {
            return Err(StoreError::InitError("the store is busy".into()))
        }
        let rootfd = cfg.rootfd;
        let file_nbit = cfg.file_nbit;
        let space_id = cfg.space_id;
        Ok(Self {
            inner: Rc::new(RefCell::new(StoreInner {
                cached_pages: lru::LruCache::new(NonZeroUsize::new(cfg.ncached_pages).expect("non-zero cache size")),
                pinned_pages: HashMap::new(),
                cached_files: lru::LruCache::new(NonZeroUsize::new(cfg.ncached_files).expect("non-zero file num")),
                file_nbit,
                rootfd,
            })),
            space_id,
        })
    }
}

impl StoreInner {
    fn fetch_page(&mut self, pid: u64) -> Result<Box<Page>, StoreError> {
        let file_size = 1 << self.file_nbit;
        let fid = (pid << PAGE_SIZE_NBIT) >> self.file_nbit;
        let file = match self.cached_files.get(&fid) {
            Some(f) => f,
            None => {
                self.cached_files.put(
                    fid,
                    File::new(fid, file_size, self.rootfd, false).map_err(StoreError::System)?,
                );
                self.cached_files.peek(&fid).unwrap()
            }
        };
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

pub struct StoreLightRef<'a> {
    pid: u64,
    data: &'a [u8],
    store: Store,
}

impl<'a> std::ops::Deref for StoreLightRef<'a> {
    type Target = [u8];
    fn deref(&self) -> &[u8] {
        self.data
    }
}

impl<'a> LinearRef for StoreLightRef<'a> {
    fn mem_image(&self) -> Box<dyn MemStore> {
        Box::new(self.store.clone())
    }
}

impl<'a> Drop for StoreLightRef<'a> {
    fn drop(&mut self) {
        self.store.inner.borrow_mut().unpin_page(self.pid);
    }
}

impl MemStore for Store {
    fn get_ref(&self, offset: u64, length: u64) -> Option<Box<dyn LinearRef>> {
        let pid = offset >> PAGE_SIZE_NBIT;
        let p_off = offset & (PAGE_SIZE - 1);
        let slice = self.inner.borrow_mut().pin_page(pid).ok()?;
        Some(Box::new(StoreLightRef {
            pid,
            data: &slice[p_off as usize..(p_off + length) as usize],
            store: self.clone(),
        }))
    }

    fn write(&self, offset: u64, change: &[u8]) {
        let pages = StoreDelta::new(
            self,
            &[DiskWrite {
                space_off: offset,
                space_id: 0, // ignored by StoreDelta::new
                data: change.into(),
            }],
        )
        .unwrap();
        let mut inner = self.inner.borrow_mut();
        for DeltaPage(pid, page) in &*pages {
            inner.update_page(*pid, page);
        }
    }

    fn id(&self) -> shale::SpaceID {
        self.space_id
    }
}

impl Drop for StoreInner {
    fn drop(&mut self) {
        flock(self.rootfd, FlockArg::UnlockNonblock).ok();
        nix::unistd::close(self.rootfd).ok();
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
