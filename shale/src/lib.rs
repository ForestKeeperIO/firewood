pub mod block;
pub mod compact;
mod util;

use std::cell::RefCell;
use std::fmt;
use std::marker::PhantomData;
use std::ops::Deref;
use std::sync::Arc;

#[derive(Debug)]
pub enum ShaleError {
    LinearMemStoreError,
    DecodeError,
}

enum MemRollback {
    Rollback(Box<[u8]>),
    NoRollback,
}

pub type SpaceID = u8;

pub struct DiskWrite {
    pub space_id: SpaceID,
    pub space_off: u64,
    pub data: Box<[u8]>,
}

/// A typed, readable handle for the stored item in [ShaleStore]. The object does not contain any
/// addressing information (and is thus read-only) and just represents the decoded/mapped data.
/// This should be implemented as part of [ShaleRefConverter::into_shale_ref].
pub trait ShaleRef<T: ?Sized>: Deref<Target = T> {
    /// Access it as a [LinearRef] object.
    fn as_linear_ref(&self) -> &dyn LinearRef;
    /// Defines how the current in-memory object of `T` should be represented in the linear storage space.
    fn to_mem_image(&self) -> Box<[u8]>;
    /// Write to the typed content.
    fn write(&mut self) -> &mut T;
    /// Returns if the typed content is memory-mapped (i.e., all changes through `write` are auto
    /// reflected in the underlying [LinearMemImage]).
    fn is_mem_mapped(&self) -> bool;
}

/// A handle that pins a portion of the linear memory image.
pub trait LinearRef: Deref<Target = [u8]> {
    /// Returns the underlying linear in-memory store.
    fn linear_memstore(&self) -> &Arc<dyn LinearMemImage>;
    /// Get a sub-interval of the linear image.
    fn slice(&self, offset: u64, length: u64) -> Box<dyn LinearRef>;
}

/// In-memory store that offers access for intervals from a linear byte space, which is usually
/// backed by a cached/memory-mapped pool of the accessed intervals from its underlying linear
/// persistent store. Reads could trigger disk reads, but writes will *only* be visible in memory
/// (it does not write back to the disk).
pub trait LinearMemImage {
    /// Returns a handle that pins the `length` of bytes starting from `offset` and makes them
    /// directly accessible.
    fn get_ref(&self, offset: u64, length: u64) -> Option<Box<dyn LinearRef>>;
    /// Overwrite the `change` to the portion of the linear space starting at `offset`.
    fn overwrite(&self, offset: u64, change: &[u8]);
    /// Returns the identifier of this storage space.
    fn id(&self) -> SpaceID;
    ///// Grows the space by a specified number of bytes and returns the original size. Use 0 to get
    ///// the current size.
    //fn grow(&self, extra: u64) -> u64;
}

/// Records a context of changes made to the [ShaleStore].
pub struct WriteContext {
    writes: RefCell<Vec<(DiskWrite, MemRollback, Arc<dyn LinearMemImage>)>>,
}

impl WriteContext {
    pub fn new() -> Self {
        Self {
            writes: RefCell::new(Vec::new()),
        }
    }

    fn add(
        &self, dw: DiskWrite, mw: MemRollback, mem: Arc<dyn LinearMemImage>,
    ) {
        self.writes.borrow_mut().push((dw, mw, mem.clone()))
    }

    /// Returns an atomic group of writes that should be done by a persistent store to make
    /// in-memory change persistent. Each write is made to one logic linear space specified
    /// by `space_id`. The `data` should be written from `space_off` in that linear space.
    pub fn commit(self) -> Vec<DiskWrite> {
        self.writes
            .into_inner()
            .into_iter()
            .map(|(dw, _, _)| dw)
            .collect()
    }

    /// Abort all changes.
    pub fn abort(self) {
        for (dw, mw, mem) in self.writes.into_inner() {
            if let MemRollback::Rollback(r) = mw {
                mem.overwrite(dw.space_off, &r);
            }
        }
    }
}

/// Opaque typed pointer in the 64-bit virtual addressable space.
#[repr(C)]
pub struct ObjPtr<T: ?Sized> {
    pub(crate) addr: u64,
    phantom: PhantomData<T>,
}

impl<T> std::cmp::PartialEq for ObjPtr<T> {
    fn eq(&self, other: &ObjPtr<T>) -> bool {
        self.addr == other.addr
    }
}

impl<T> Eq for ObjPtr<T> {}
impl<T> Copy for ObjPtr<T> {}
impl<T> Clone for ObjPtr<T> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<T> fmt::Display for ObjPtr<T> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "[ObjPtr addr={:08x}]", self.addr)
    }
}

impl<T: ?Sized> ObjPtr<T> {
    pub fn null() -> Self {
        Self::new(0)
    }
    pub fn is_null(&self) -> bool {
        self.addr == 0
    }
    pub fn addr(&self) -> u64 {
        self.addr
    }

    #[inline(always)]
    pub(crate) fn new(addr: u64) -> Self {
        ObjPtr {
            addr,
            phantom: PhantomData,
        }
    }

    #[inline(always)]
    pub unsafe fn new_from_addr(addr: u64) -> Self {
        Self::new(addr)
    }
}

impl<T> std::hash::Hash for ObjPtr<T> {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.addr.hash(state)
    }
}

/// Handle offers read & write access to the stored [ShaleStore] item.
pub struct ObjRef<'a, T: ?Sized> {
    addr: u64,
    space_id: SpaceID,
    inner: Box<dyn ShaleRef<T> + 'a>,
}

impl<'a, T: ?Sized> ObjRef<'a, T> {
    #[inline(always)]
    pub fn as_ptr(&self) -> ObjPtr<T> {
        ObjPtr::<T>::new(self.addr)
    }

    pub unsafe fn to_longlive(self) -> ObjRef<'static, T> {
        let inner = Box::into_raw(self.inner);
        ObjRef {
            addr: self.addr,
            space_id: self.space_id,
            inner: Box::from_raw(std::mem::transmute::<
                *mut (dyn ShaleRef<T> + 'a),
                *mut (dyn ShaleRef<T> + 'static),
            >(inner)),
        }
    }
}

impl<'a, T: ?Sized> ObjRef<'a, T> {
    pub fn write(
        &mut self, modify: impl FnOnce(&mut T) -> (), mem_rollback: bool,
        wctx: &WriteContext,
    ) {
        let mw = if mem_rollback {
            MemRollback::Rollback(self.inner.to_mem_image())
        } else {
            MemRollback::NoRollback
        };
        modify(self.inner.write());
        let lspace = self.inner.as_linear_ref().linear_memstore().clone();
        let new_value = self.inner.to_mem_image();
        if !self.inner.is_mem_mapped() {
            lspace.overwrite(self.addr, &new_value);
        }
        if new_value.len() == 0 {
            return
        }
        let dw = DiskWrite {
            space_off: self.addr,
            space_id: self.space_id,
            data: new_value,
        };
        wctx.add(dw, mw, lspace)
    }

    pub fn get_space_id(&self) -> SpaceID {
        self.space_id
    }

    pub unsafe fn from_shale(
        addr: u64, space_id: SpaceID, r: Box<dyn ShaleRef<T> + 'a>,
    ) -> Self {
        ObjRef {
            addr,
            space_id,
            inner: r,
        }
    }
}

impl<'a, T> Deref for ObjRef<'a, T> {
    type Target = T;
    fn deref(&self) -> &T {
        &*self.inner
    }
}

/// A persistent item storage backed by linear logical space. New items can be created and old
/// items could be dropped or retrieved.
pub trait ShaleStore {
    /// Derference the [ObjPtr] to a handle that gives direct access to the item in memory.
    fn get_item<'a, T: MummyItem + 'a>(
        &'a self, ptr: ObjPtr<T>,
    ) -> Result<ObjRef<'a, T>, ShaleError>;
    /// Allocate a new item.
    fn put_item<'a, T: MummyItem + 'a>(
        &'a self, item: T, wctx: &WriteContext,
    ) -> Result<ObjRef<'a, T>, ShaleError>;
    /// Free a item and recycle its space when applicable.
    fn free_item<T: MummyItem>(
        &mut self, item: ObjPtr<T>, wctx: &WriteContext,
    ) -> Result<(), ShaleError>;
}

/// A stored item type that could be decoded from or encoded to on-disk raw bytes. An efficient
/// implementation could be directly transmuting to/from a POD struct. But sometimes necessary
/// compression/decompression is needed to reduce disk I/O and facilitate faster in-memory access.
pub trait MummyItem {
    fn dehydrate(&self) -> Vec<u8>;
    fn hydrate(
        addr: u64, mem: &dyn LinearMemImage,
    ) -> Result<(u64, Self), ShaleError>
    where
        Self: Sized;
    fn is_mem_mapped(&self) -> bool {
        false
    }
}

/// Helper implementation of [ShaleRef]. It takes any type that implements [MummyItem] and should
/// be used in most of the circumstances.
pub struct MummyRef<T> {
    decoded: T,
    raw: Box<dyn LinearRef>,
}

impl<T> Deref for MummyRef<T> {
    type Target = T;
    fn deref(&self) -> &T {
        &self.decoded
    }
}

impl<T: MummyItem> ShaleRef<T> for MummyRef<T> {
    fn as_linear_ref(&self) -> &dyn LinearRef {
        &*self.raw
    }
    fn to_mem_image(&self) -> Box<[u8]> {
        self.decoded.dehydrate().into()
    }
    fn write(&mut self) -> &mut T {
        &mut self.decoded
    }
    fn is_mem_mapped(&self) -> bool {
        self.decoded.is_mem_mapped()
    }
}

impl<T: MummyItem> MummyRef<T> {
    fn new(addr: u64, space: &dyn LinearMemImage) -> Result<Self, ShaleError> {
        let (length, decoded) = T::hydrate(addr, space)?;
        Ok(Self {
            decoded,
            raw: space
                .get_ref(addr, length)
                .ok_or(ShaleError::LinearMemStoreError)?,
        })
    }
}

impl<T> MummyRef<T> {
    unsafe fn new_from_slice(
        addr: u64, length: u64, decoded: T, space: &dyn LinearMemImage,
    ) -> Result<Self, ShaleError> {
        Ok(Self {
            decoded,
            raw: space
                .get_ref(addr, length)
                .ok_or(ShaleError::LinearMemStoreError)?,
        })
    }

    pub unsafe fn slice<'b, U: MummyItem + 'b>(
        s: &ObjRef<'b, T>, offset: u64, length: u64, decoded: U,
    ) -> Result<ObjRef<'b, U>, ShaleError> {
        let addr = s.addr + offset;
        let r = Box::new(MummyRef::new_from_slice(
            addr,
            length,
            decoded,
            s.inner.as_linear_ref().linear_memstore().as_ref(),
        )?);
        Ok(ObjRef {
            addr,
            space_id: s.space_id,
            inner: r,
        })
    }
}

impl<T> MummyItem for ObjPtr<T> {
    fn dehydrate(&self) -> Vec<u8> {
        self.addr().to_le_bytes().into()
    }

    fn hydrate(
        addr: u64, mem: &dyn LinearMemImage,
    ) -> Result<(u64, Self), ShaleError> {
        const SIZE: u64 = 8;
        let raw = mem
            .get_ref(addr, SIZE)
            .ok_or(ShaleError::LinearMemStoreError)?;
        unsafe {
            Ok((
                SIZE,
                Self::new_from_addr(u64::from_le_bytes(
                    (**raw).try_into().unwrap(),
                )),
            ))
        }
    }
}
