use std::fmt;
use std::sync::Arc;

use nix::fcntl::{flock, FlockArg};
use shale::{DiskWrite, LinearRef, MemStore};
use typed_builder::TypedBuilder;

use crate::file::{Fd, File};

const PAGE_SIZE_NBIT: u64 = 12;
const PAGE_SIZE: u64 = 1 << PAGE_SIZE_NBIT;

/// Basic copy-on-write item in the linear storage space for multi-versioning.
struct StorePage(u64, [u8; PAGE_SIZE as usize]);

pub enum StoreSource {
    Internal(StoreRev),
    External(Arc<dyn MemStore>),
}

impl StoreSource {
    fn get_ref(&self, offset: u64, length: u64) -> Option<Box<dyn LinearRef>> {
        match self {
            Self::Internal(rev) => rev.get_ref(offset, length),
            Self::External(store) => store.get_ref(offset, length),
        }
    }
}

struct StoreDelta(Vec<StorePage>);

impl std::ops::Deref for StoreDelta {
    type Target = [StorePage];
    fn deref(&self) -> &[StorePage] {
        &self.0
    }
}

impl StoreDelta {
    pub fn new(src: &StoreSource, writes: &[DiskWrite]) -> Option<Self> {
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
                    deltas.push(StorePage(
                        off,
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
                (*if w.space_off < deltas[mid].0 { &mut r } else { &mut l }) = mid;
            }
            let off = (w.space_off - deltas[l].0) as usize;
            let len = std::cmp::min(psize - off, w.data.len());
            deltas[l].1[off..off + len].copy_from_slice(&w.data[..len]);
            let mut data = &w.data[len..];
            while data.len() >= psize {
                l += 1;
                assert!(w.space_off <= deltas[l].0 && deltas[l].0 + PAGE_SIZE <= w.space_off + w.data.len() as u64);
                deltas[l].1.copy_from_slice(&data[..psize]);
                data = &data[psize..];
            }
            if data.len() > 0 {
                l += 1;
                deltas[l].1[..data.len()].copy_from_slice(&data);
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
        let deltas = StoreDelta::new(&prev, writes)?;
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
            return self.0.prev.get_ref(start, end - start)
        }
        // otherwise, some dirty pages are covered by the range
        while r - l > 1 {
            let mid = (l + r) >> 1;
            (*if start < deltas[mid].0 { &mut r } else { &mut l }) = mid;
        }
        if start >= deltas[l].0 + PAGE_SIZE {
            l += 1
        }
        if l >= deltas.len() || end < deltas[l].0 {
            return self.0.prev.get_ref(start, end - start)
        }
        let mut data = Vec::new();
        let p_off = std::cmp::min(end - deltas[l].0, PAGE_SIZE);
        if start < deltas[l].0 {
            data.extend(self.0.prev.get_ref(start, deltas[l].0 - start)?.deref());
            data.extend(&deltas[l].1[..p_off as usize]);
        } else {
            data.extend(&deltas[l].1[(start - deltas[l].0) as usize..p_off as usize]);
        };
        start = deltas[l].0 + p_off;
        while start < end {
            l += 1;
            if l >= deltas.len() || end < deltas[l].0 {
                data.extend(self.0.prev.get_ref(start, end - start)?.deref());
                break
            }
            if deltas[l].0 > start {
                data.extend(self.0.prev.get_ref(start, deltas[l].0 - start)?.deref());
            }
            if end < deltas[l].0 + PAGE_SIZE {
                data.extend(&deltas[l].1[..(end - deltas[l].0) as usize]);
                break
            }
            data.extend(&deltas[l].1);
            start = deltas[l].0 + PAGE_SIZE;
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
}

struct StoreInner {
    cached_pages: lru::LruCache<u64, [u8; PAGE_SIZE as usize]>,
    cached_files: lru::LruCache<u64, File>,
    file_nbit: u64,
    rootfd: Fd,
}

pub struct Store(Arc<StoreInner>);

impl Store {
    pub fn new(cfg: StoreConfig) -> Result<Self, StoreError> {
        use std::num::NonZeroUsize;
        if let Err(_) = flock(cfg.rootfd, FlockArg::LockExclusiveNonblock) {
            return Err(StoreError::InitError("the store is busy".into()))
        }
        let rootfd = cfg.rootfd;
        let file_nbit = cfg.file_nbit;
        Ok(Self(Arc::new(StoreInner {
            cached_pages: lru::LruCache::new(NonZeroUsize::new(cfg.ncached_pages).expect("non-zero cache size")),
            cached_files: lru::LruCache::new(NonZeroUsize::new(cfg.ncached_files).expect("non-zero file num")),
            file_nbit,
            rootfd,
        })))
    }
}

pub struct StoreLightRef<'a> {
    data: &'a [u8],
    store: Arc<StoreInner>,
}

impl MemStore for Store {
    fn get_ref(&self, offset: u64, length: u64) -> Option<Box<dyn LinearRef>> {
        unimplemented!()
    }

    fn write(&self, _offset: u64, _change: &[u8]) {
        unimplemented!()
    }

    fn id(&self) -> shale::SpaceID {
        shale::INVALID_SPACE_ID
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
