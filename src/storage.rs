use shale::{DiskWrite, LinearRef, MemStore};
use std::fmt;
use std::sync::Arc;

const PAGE_SIZE_NBIT: u64 = 12;
const PAGE_SIZE: u64 = 1 << PAGE_SIZE_NBIT;

/// Basic copy-on-write item in the linear storage space for multi-versioning.
struct StoragePage(u64, [u8; PAGE_SIZE as usize]);

pub enum StoreSource {
    Internal(StorageRev),
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

pub struct StorageRevInner {
    prev: StoreSource,
    deltas: Vec<StoragePage>,
}

#[derive(Clone)]
pub struct StorageRev(Arc<StorageRevInner>);

impl fmt::Debug for StorageRev {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "[StorageRev")?;
        for d in self.0.deltas.iter() {
            write!(f, " 0x{:x}-0x{:x}", d.0, d.0 + PAGE_SIZE)?;
        }
        write!(f, "]\n")
    }
}

impl StorageRev {
    pub fn apply_change(src: StoreSource, writes: &[DiskWrite]) -> Option<StorageRev> {
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
                    deltas.push(StoragePage(
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

        for d in deltas.iter() {
            println!("0x{:x}-0x{:x}", d.0, d.0 + PAGE_SIZE);
        }

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
        let inner = StorageRevInner { prev: src, deltas };
        Some(Self(Arc::new(inner)))
    }
}

struct StorageRef<S: Clone + MemStore> {
    data: Vec<u8>,
    store: S,
}

impl<S: Clone + MemStore> std::ops::Deref for StorageRef<S> {
    type Target = [u8];
    fn deref(&self) -> &[u8] {
        &self.data
    }
}

impl<S: Clone + MemStore + 'static> LinearRef for StorageRef<S> {
    fn mem_image(&self) -> Box<dyn MemStore> {
        Box::new(self.store.clone())
    }
}

impl MemStore for StorageRev {
    fn get_ref(&self, offset: u64, length: u64) -> Option<Box<dyn LinearRef>> {
        let mut start = offset;
        let end = start + length;
        let deltas = &self.0.deltas;
        let mut l = 0;
        let mut r = deltas.len();
        // no dirty page, before or after all dirty pages
        if r == 0 || start >= deltas[r - 1].0 + PAGE_SIZE || end <= deltas[0].0 {
            return self.0.prev.get_ref(start, end)
        }
        // otherwise, some dirty pages are covered by the range
        while r - l > 1 {
            let mid = (l + r) >> 1;
            (*if start < deltas[mid].0 { &mut r } else { &mut l }) = mid;
        }
        let mut data = Vec::new();
        let p_off = std::cmp::min(end - deltas[l].0, PAGE_SIZE);
        if start < deltas[l].0 {
            data.extend(self.0.prev.get_ref(start, deltas[l].0 - start)?.deref());
            data.extend(&deltas[l].1[..p_off as usize]);
        } else {
            data.extend(&deltas[l].1[(start - deltas[l].0) as usize..p_off as usize]);
        }
        start = deltas[l].0 + p_off;
        while start < end {
            l += 1;
            if l >= deltas.len() {
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
        if data.len() != length as usize {
            println!("{} {}", data.len(), length);
        }
        assert!(data.len() == length as usize);

        let store = self.clone();
        Some(Box::new(StorageRef { data, store }))
    }

    fn write(&self, _offset: u64, _change: &[u8]) {
        panic!("StorageRev should not be written");
    }

    fn id(&self) -> shale::SpaceID {
        shale::INVALID_SPACE_ID
    }
}

#[derive(Clone)]
pub struct ZeroStorage(Arc<()>);

impl ZeroStorage {
    pub fn new() -> Self {
        Self(Arc::new(()))
    }
}

impl MemStore for ZeroStorage {
    fn get_ref(&self, _: u64, length: u64) -> Option<Box<dyn LinearRef>> {
        Some(Box::new(StorageRef {
            data: vec![0; length as usize],
            store: self.clone(),
        }))
    }

    fn write(&self, _: u64, _: &[u8]) {
        panic!("ZeroStorage should not be written");
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
    for _ in 0..1000 {
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
        let z = Arc::new(ZeroStorage::new());
        let rev = StorageRev::apply_change(StoreSource::External(z), &writes).unwrap();
        let ans = rev.get_ref(min, max - min).unwrap();
        assert_eq!(ans.deref(), &*canvas);
    }
}
