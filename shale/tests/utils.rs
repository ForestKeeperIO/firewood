use std::{
    borrow::Borrow,
    cell::{RefCell, UnsafeCell},
    fs::OpenOptions,
    io::{Cursor, Seek, SeekFrom, Write},
    ops::{Deref, DerefMut},
    os::fd::RawFd,
    path::{Path, PathBuf},
    rc::Rc,
};

use growthring::oflags;
use memmap2::MmapMut;
use nix::{
    errno::Errno,
    fcntl::{open, openat},
    sys::stat::Mode,
    unistd::mkdir,
};
use rand::{thread_rng, Rng};
use shale::{MemStore, MemView, MummyItem, ShaleError, SpaceID};

const HASH_SIZE: usize = 32;

#[derive(PartialEq, Eq, Debug, Clone)]
pub struct Hash(pub [u8; HASH_SIZE]);

impl Hash {
    const MSIZE: u64 = HASH_SIZE as u64;
}

impl MummyItem for Hash {
    fn hydrate<T: MemStore>(addr: u64, mem: &T) -> Result<Self, ShaleError> {
        let raw = mem
            .get_view(addr, Self::MSIZE)
            .ok_or(ShaleError::LinearMemStoreError)?;
        Ok(Self(
            raw.as_deref()[..Self::MSIZE as usize].try_into().unwrap(),
        ))
    }

    fn dehydrated_len(&self) -> u64 {
        Self::MSIZE
    }

    fn dehydrate(&self, to: &mut [u8]) {
        Cursor::new(to).write_all(&self.0).unwrap()
    }
}

pub fn touch_dir(dirname: &str, rootfd: RawFd) -> Result<RawFd, Errno> {
    use nix::sys::stat::mkdirat;
    if mkdirat(
        rootfd,
        dirname,
        Mode::S_IRUSR | Mode::S_IWUSR | Mode::S_IXUSR,
    )
    .is_err()
    {
        let errno = nix::errno::from_i32(nix::errno::errno());
        if errno != nix::errno::Errno::EEXIST {
            return Err(errno);
        }
    }
    openat(rootfd, dirname, oflags(), Mode::empty())
}

pub fn open_dir<P: AsRef<Path>>(path: P, truncate: bool) -> Result<(RawFd, bool), nix::Error> {
    let mut reset_header = truncate;
    if truncate {
        let _ = std::fs::remove_dir_all(path.as_ref());
    }
    match mkdir(path.as_ref(), Mode::S_IRUSR | Mode::S_IWUSR | Mode::S_IXUSR) {
        Err(e) => {
            if truncate {
                return Err(e);
            }
        }
        Ok(_) => {
            // the DB did not exist
            reset_header = true
        }
    }
    Ok((
        match open(path.as_ref(), oflags(), Mode::empty()) {
            Ok(fd) => fd,
            Err(e) => return Err(e),
        },
        reset_header,
    ))
}

#[derive(Debug)]
pub struct MappedMem {
    space: MmapMut,
    id: SpaceID,
}

impl MappedMem {
    pub fn new(mmap: MmapMut, id: SpaceID) -> Self {
        let space = mmap;
        Self { space, id }
    }
}

impl MemStore for MappedMem {
    fn get_view(
        &self,
        offset: u64,
        length: u64,
    ) -> Option<Box<dyn MemView<DerefReturn = Vec<u8>>>> {
        let offset = offset as usize;
        let length = length as usize;
        let size = offset + length;
        let data: &[u8] = &self.space[offset..size];

        Some(Box::new(StoreRef {
            data: data.to_vec(),
        }))
    }

    fn get_shared(&self) -> Option<Box<dyn DerefMut<Target = dyn MemStore>>> {
        todo!()
    }

    fn write(&mut self, offset: u64, change: &[u8]) {
        let offset = offset as usize;
        let length = change.len();
        let size = offset + length;
        self.space[offset..size].copy_from_slice(change)
    }

    fn id(&self) -> SpaceID {
        self.id
    }
}

struct StoreShared<S: Clone + MemStore>(S);

impl<S: Clone + MemStore + 'static> Deref for StoreShared<S> {
    type Target = dyn MemStore;
    fn deref(&self) -> &(dyn MemStore + 'static) {
        &self.0
    }
}

impl<S: Clone + MemStore + 'static> DerefMut for StoreShared<S> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

#[derive(Debug)]
struct DynamicMemView {
    offset: usize,
    length: usize,
    mem: MappedMem,
}

// struct DynamicMemShared(DynamicMem);

// impl Deref for MappedMem {
//     type Target = dyn MemStore;
//     fn deref(&self) -> &(dyn MemStore + 'static) {
//         &self.space.into()
//     }
// }

// impl DerefMut for MappedMem {
//     fn deref_mut(&mut self) -> &mut Self::Target {
//         &mut self.0
//     }
// }

// impl MemView for DynamicMemView {
//     type DerefReturn = Vec<u8>;

//     fn as_deref(&self) -> Self::DerefReturn {
//         self.mem.get_space_mut()[self.offset..self.offset + self.length].to_vec()
//     }
// }

#[derive(Debug)]
struct StoreRef {
    data: Vec<u8>,
}

impl Deref for StoreRef {
    type Target = [u8];
    fn deref(&self) -> &[u8] {
        &self.data
    }
}

impl MemView for StoreRef {
    type DerefReturn = Vec<u8>;

    fn as_deref(&self) -> Self::DerefReturn {
        self.deref().to_vec()
    }
}

#[allow(dead_code)]
pub(crate) enum IncludeHeader {
    /// caller wants header
    Header,
    /// caller wants header skipped
    NoHeader,
}

pub struct Storage {
    path: PathBuf,
    mmap: MmapMut,
    pub cell_size: u64,
    pub count: Arc<AtomicU64>,
    pub stats: Arc<BucketStats>,
    pub max_search: MaxSearch,
    pub contents: O,
}

impl<O: BucketOccupied> Drop for BucketStorage<O> {
    fn drop(&mut self) {
        _ = remove_file(&self.path);
    }
}

impl Storage {
    fn new(path: PathBuf, bytes: u64) -> (MmapMut, PathBuf) {
        let mut data = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&path)
            .map_err(|e| {
                panic!(
                    "Unable to create data file {} in current dir({:?}): {:?}",
                    path.display(),
                    std::env::current_dir(),
                    e
                );
            })
            .unwrap();

        data.seek(SeekFrom::Start(bytes - 1)).unwrap();
        data.write_all(&[0]).unwrap();
        data.rewind().unwrap();
        data.flush().unwrap(); // can we skip this?
        let res = (unsafe { MmapMut::map_mut(&data).unwrap() }, path);
        res
    }

    fn get_start_offset_with_header(&self, ix: u64) -> usize {
        assert!(ix < self.capacity(), "bad index size");
        (self.cell_size * ix) as usize
    }

    /// Return the number of cells currently allocated
    pub fn capacity(&self) -> u64 {
        self.contents.capacity()
    }

    pub(crate) fn get_mut_cell_slice<T>(
        &mut self,
        ix: u64,
        len: u64,
        header: IncludeHeader,
    ) -> &mut [T] {
        let start = self.get_start_offset(ix, header);
        let slice = {
            let size = std::mem::size_of::<T>() * len as usize;
            let slice = &mut self.mmap[start..];
            debug_assert!(slice.len() >= size);
            &mut slice[..size]
        };
        let ptr = {
            let ptr = slice.as_mut_ptr() as *mut T;
            debug_assert!(ptr as usize % std::mem::align_of::<T>() == 0);
            ptr
        };
        unsafe { std::slice::from_raw_parts_mut(ptr, len as usize) }
    }


}

#[test]
fn tests() {
    let path = PathBuf::from("/tmp/stuffs");
    let (mmap, path) = new_map(path, 10);

    mmap.get(index)
}
