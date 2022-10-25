//! # Firewood: non-archival blockchain key-value store with hyper-fast recent state retrieval.
//!
//! Firewood is an embedded key-value store, optimized to store blockchain state. It prioritizes
//! access to latest state, by providing extremely fast reads, but also provides a limited view
//! into past state. It does not copy-on-write the Merkle Patricia Trie (MPT) to generate an ever
//! growing forest of tries like EVM, but instead keeps one latest version of the MPT index on disk
//! and apply in-place updates to it. This ensures that the database size is small and stable
//! during the course of running firewood. Firewood was first conceived to provide a very fast
//! storage layer for qEVM to enable a fast, complete EVM system with right design choices made
//! totally from scratch, but it also serves as a drop-in replacement for any EVM-compatible
//! blockchain storage system, and fits for the general use of a certified key-value store of
//! arbitrary data.
//!
//! Firewood is a robust database implemented from the ground up to directly store MPT nodes and
//! user data. Unlike most (if not all) of the solutions in the field, it is not built on top of a
//! generic KV store such as LevelDB/RocksDB. Like a B+-tree based store, firewood directly uses
//! the tree structure as the index on disk. Thus, there is no additional "emulation" of the
//! logical MPT to flatten out the data structure to feed into the underlying DB that is unaware
//! of the data being stored.
//!
//! Firewood provides OS-level crash recovery via a write-ahead log (WAL). The WAL guarantees
//! atomicity and durability in the database, but also offers "reversibility": some portion
//! of the old WAL can be optionally kept around to allow a fast in-memory rollback to recover
//! some past versions of the entire store back in memory. While running the store, new changes
//! will also contribute to the configured window of changes (at batch granularity) to access any past
//! versions with no additional cost at all.
//!
//! The on-disk footprint of Firewood is more compact than geth. It provides two isolated storage
//! space which can be both or selectively used the user. The account model portion of the storage
//! offers something very similar to `StateDB` in geth, which captures the address-"state key"
//! style of two-level access for an account's (smart contract's) state. Therefore, it takes
//! minimal effort to delegate all state storage from an EVM implementation to firewood. The other
//! portion of the storage supports generic MPT storage for arbitrary keys and values. When unused,
//! there is no additional cost.
//!
//! # Design Philosophy & Overview
//!
//! With some on-going academic research efforts and increasing demand of faster local storage
//! solutions for the chain state, we realized there are mainly two different regimes of designs.
//!
//! - "Archival" Storage: this style of design emphasizes on the ability to hold all historical
//!   data and retrieve a revision of any wold state at a reasonable performance. To economically
//!   store all historical certified data, usually copy-on-write merkle tries are used to just
//!   capture the changes made by a committed block. The entire storage consists of a forest of these
//!   "delta" tries. The total size of the storage will keep growing over the chain length and an ideal,
//!   well-executed plan for this is to make sure the performance degradation is reasonable or
//!   well-contained with respect to the ever-increasing size of the index. This design is useful
//!   for nodes which serve as the backend for some indexing service (e.g. chain explorer) or as a
//!   query portal to some user agent (e.g. wallet apps). Blockchains with poor finality may also
//!   need this because the "canonical" branch of the chain could switch (but not necessarily a
//!   practical concern nowadays) to a different fork at times.
//!
//! - "Validation" Storage: this regime optimizes for the storage footprint and the performance of
//!   operations upon the latest/recent states. With the assumption that the chain's total state
//!   size is relatively stable over ever-coming blocks, one can just make the latest state
//!   persisted and available to the blockchain system as that's what matters for most of the time.
//!   While one can still keep some volatile state versions in memory for mutation and VM
//!   execution, the final commit to some state works on a singleton so the indexed merkle tries
//!   may be typically updated in place. It is also possible (e.g., firewood) to allow some
//!   infrequent access to historical versions with higher cost, and/or allow fast access to
//!   versions of the store within certain limited recency. This style of storage is useful for
//!   the blockchain systems where only (or mostly) the latest state is required and data footprint
//!   should remain constant or grow slowly if possible for sustainability. Validators who
//!   directly participate in the consensus and vote for the blocks, for example, can largely
//!   benefit from such a design.
//!
//! In firewood, we take a closer look at the second regime and have come up with a simple but
//! robust architecture that fulfills the need for such blockchain storage.
//!
//! ## Storage Model
//!
//! Firewood is built by three layers of abstractions that totally decouple the
//! layout/representation of the data on disk from the actual logical data structure it retains:
//!
//! - Linear, memory-like space: the `shale` crate from an academic project (CedrusDB) code
//!   offers an abstraction for a (64-bit) byte-addressable space that abstracts away the intricate
//!   method that actually persists the in-memory data on the secondary storage medium (e.g., hard
//!   drive) named `MemStore`. The implementor of `MemStore` will provide the functions to give the
//!   user of `MemStore` an illusion as if the user is operating upon a byte-addressable memory
//!   space. In short, it is just a "magical" array of bytes one can view and change that also
//!   mirrors to the disk. In reality, the linear space will be chunked into files under a
//!   directory, but the user does not have to even know about this.
//!
//! - Persistent item storage stash: `ShaleStore` trait from `shale` defines a typed pool of
//!   objects that are persisted on disk but also made accessible in memory transparently. It is
//!   built on top of `MemStore` by defining how "items" of the given type are laid out, allocated
//!   and recycled throughout their life cycles (there is a disk-friendly, malloc-style kind of
//!   basic implementation in `shale` crate, but one can always define his/her own `ShaleStore`).
//!
//! - Data structure: in Firewood, one or more Ethereum-style MPTs are maintained by invoking
//!   `ShaleStore` (see `src/merkle.rs`, another stash is for code objects in `src/account.rs`).
//!   The data structure code is totally unaware of how its objects (i.e. nodes) are organized or
//!   persisted on disk. It is as if they're just in memory, which makes it much easier to write
//!   and maintain the code.
//!
//! The three layers are depicted as follows:
//!
//! <p align="center">
//!     <img src="https://drive.google.com/uc?export=view&id=1KnlpqnxkmFd_aKZHwcferIdX137GVZJr" width="80%">
//! </p>
//!
//! Given the abstraction, one can easily realize the fact that the actual data that affects the
//! state of the data structure (MPT) is what the linear space (`MemStore`) keeps track of, that is,
//! a flat but conceptually large byte vector. In other words, given a valid byte vector as the
//! content of the linear space, the higher level data structure can be *uniquely* determined, there
//! is nothing more (except for some auxiliary data that are kept for performance, such as caching)
//! or less than that, like a way to view the bytes. This nice property allows us to completely
//! separate the logical data from its physical representation, and greatly simplifies the storage
//! code. It is also very generic and versatile, as in theory any persistent data could be stored
//! this way -- sometimes you need a different `MemShale` or `MemStore` implementation, but you do
//! not even have to touch the code for the persisted data structure.
//!
//! ## Page-based Shadowing and Revisions
//!
//! Following the idea that the MPTs are just a view of a linear byte space, all writes made to the
//! MPTs inside Firewood will eventually be consolidated into some interval writes to the linear
//! space. The writes may overlap and some frequent writes are even done to the same spot in the
//! space. To reduce the overhead and be friendly to the disk, we partition the entire 64-bit
//! virtual space into pages (yeah it appears to be more and more like an OS) and keep track of the
//! dirty pages in `MemStore` (see `storage::StoreRevMut` which implements `MemStore`). When a
//! [`db::WriteBatch`] commits, both the recorded interval writes and the aggregated in-memory
//! dirty pages induced by this write batch are taken out from the linear space. Although they are
//! mathematically equivalent, interval writes are more compact (suppose there are not a lot of
//! overlaps) than pages (which are 4K in size, become dirty even if a single byte is touched upon). So
//! interval writes are fed into the WAL subsystem (supported by `growthring`). After the WAL
//! record is written (one record per write batch), the dirty pages are then pushed to the on-disk
//! linear space to mirror the change by some asynchronous, out-of-order file writes. See the
//! `BufferCmd::WriteBatch` part of `DiskBuffer::process` for the detailed logic.
//!
//! In short, a Read-Modify-Write (RMW) style normal operation flow is as follows in Firewood:
//!
//! - Traverse the MPT, and that induces the access to the nodes. Suppose the nodes are not already in
//!   memory, then:
//!
//! - Bring the necessary pages that contain the accessed nodes into the memory and cache them
//!   (`storage::CachedSpace`).
//!
//! - Make changes to the MPT, and that induces the writes to the nodes. The nodes are either
//!   already cached in memory (either its pages are cached, or its handle `ObjRef<Node>` is still in
//!   `shale::ObjCache`) or need to bring into the memory (if that's the case, go back to second step for it).
//!
//! - Writes to nodes are converted into interval writes to the stagging `StoreRevMut` space that
//!   overlays atop `CachedSpace`, so all dirty pages during the current write batch will be
//!   exactly captured in `StoreRevMut` (see `StoreRevMut::take_delta`).
//!
//! - Finally:
//!
//!   - If the write batch is dropped without invoking `db::WriteBatch::commit`, all in-memory
//!     changes will be aborted, the dirty pages from `StoreRevMut` will be dropped and the merkle
//!     will "revert" back to its original state without having to actually change anything.
//!
//!   - Otherwise, the write batch is committed, the interval writes (`storage::Ash`) will be bundled
//!     into a single WAL record (`storage::AshRecord`) and sent to WAL subsystem, before dirty pages
//!     are scheduled to be written to the space files. Also the dirty pages are applied to the
//!     underlying `CachedSpace` and `StoreRevMut` becomes empty again for further write batches.
//!
//! Parts of the following diagram depicts this normal flow, the "staging area" (implemented by
//! `StoreRevMut`) concept is a bit similar to Git and that allows handling of (resuming from)
//! operational errors and clean abort of an on-going write batch made to the entire store state.
//! Essentially, we copy-on-write pages in the space that are touched upon, without directly
//! mutating the underlying master space revision. The staging space is just a collection of these
//! "shadowing" pages and a reference to the its base (master) so any reads could partially hit those
//! dirty pages or just fall through to the base, whereas all writes are captured. Finally, when
//! things go well, we "push down" these changes to the base and clear up the staging space.
//!
//! <p align="center">
//!     <img src="https://drive.google.com/uc?export=view&id=1l2CUbq85nX_g0GfQj44ClrKXd253sBFv" width="100%">
//! </p>
//!
//! Thanks to the shadow pages, we can both revive some historical revision of the store and
//! maintain a rolling window of past revisions on-the-fly. The diagram shows previously logged
//! write batch records could be kept even though they are no longer needed for the purpose of
//! crash recovery. The interval writes from a record can be aggregated into pages (see `storage::
//! StoreDelta::new`) and used to reconstruct a "ghost" image of past revision of the linear space
//! (just like how staging space works, except that writes will be ignored so the ghost space is
//! essentially read-only). The shadow pages there will function as some "rewinding" changes to
//! patch the necessary locations in the linear space, while the rest of the linear space is
//! very likely untouched by that historical write batch.
//!
//! Then, with the three-layer abstraction we previously talked about, an historical MPT could be
//! derived. In fact, because there is no mandatory traversal or scanning for this process, the
//! only cost to revive a historical state from the log is to just playback the records and create
//! those shadow pages. There is very little cost because the ghost space is invoked on an
//! on-demand manner while one accesses the historical MPT.
//!
//! In the other direction, when new write batches are committed, the system moves forward, we can
//! also maintain a rolling window of past revisions in memory with *zero* cost. The bottom of the
//! diagram shows when a write batch is committed, the persisted space goes one step forward,
//! the staging space is cleared, and an extra ghost space can be created to hold the version of
//! the store before the commit. The backward delta is applied to counteract the change that is
//! made to the persisted store, which is also a set of shadow pages. No change is required for
//! other historical ghost space instances. Finally, we can phase out some very old ghost space to
//! keep the size invariant of the rolling window.
//!
pub(crate) mod account;
pub mod db;
pub(crate) mod file;
pub mod merkle;
pub(crate) mod storage;
