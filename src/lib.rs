//! # Firewood: non-archival blockchain key-value store with hyper-fast recent state retrieval.
//!
//! Firewood is an embedded key-value store, optimized to store blockchain state. It prioritizes
//! access to latest state, by providing extremely fast reads, but also provides a limited view
//! into past state. It does not copy-on-write the (Merkle Patricia Trie) MPT to generate an ever
//! growing forest of tries like EVM, but instead keeps one latest version of the MPT index on disk
//! and apply in-place updates to it. This ensures that the database size is small and stable
//! during the course of running firewood. Firewood was first conceived to provide a very fast
//! storage layer for qEVM to enable a fast, complete EVM system with right design choices made
//! totally from scratch, but it also serves as a drop-in replacement for any EVM-compatible
//! blockchain storage system, and fits for the generic use of a certified key-value store of
//! arbitrary data.
//!
//! Firewood is a robust database implemented from the ground up to directly stores MPT nodes and
//! user data. Unlike most (if not all) of the solutions in the field, it is not built on top of a
//! generic KV store such as LevelDB/RocksDB. Like a B+-tree based store, firewood directly uses
//! MPT as the index on disk. Thus, there is no additional "emulation" of the logical MPT to
//! flatten out the data structure to be stored in the underlying DB that is unaware of the
//! locality.
//!
//! Firewood provides OS-level crash recovery via a write-ahead-log (WAL). The WAL guarantees
//! atomicity and durability in the database, but also offers "reversibility": some portion
//! of the old WAL can be optionally kept and pruned to allow a fast in-memory rollback to recover
//! some past revisions of the entire store on-the-fly. While running the store, new changes will
//! also maintain the configured window of changes (batch-granularity) to access any past revisions
//! with no cost at all.
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
//! With some on-going academic research efforts and increasing demand of faster blockchain
//! local storage solutions for the chain state, we realized there are mainly two different
//! regimes in design.
//!
//! - "Archival" Storage: this style of design emphasizes on the ability to hold all historical
//!   data and retrieve a revision of any wold state at a reasonable performance. To economically
//!   store all historical certified data, usually copy-on-write merkle tries are used to just
//!   capture the changes made to the state. The entire storage will be made of a forest of these
//!   delta tries. The total size of the storage will keep growing over time and an ideal,
//!   well-executed plan for this is to make sure the performance degradation is reasonable or
//!   well-contained with respect to the increasing size of the storage. It is useful in archival
//!   nodes which serve as the backend for some indexing service (e.g. chain explorer) or as a
//!   query portal to some user agent (e.g. wallet apps). Blockchains with poor finality may also
//!   need this because the "canonical" branch of the chain could switch (but not necessarily a
//!   practical concern nowadays) to a different fork at times.
//!
//! - "Validation" Storage: this regime optimizes for the storage footprint and the performance of
//!   operations upon the latest state. With the assumption that the chain's global (world) state
//!   size is relatively stable over ever-coming blocks, one can just make the latest state persisted and
//!   available to the blockchain system as that's what matters for most of the time. While one can
//!   still keep some volatile state revisions in-memory for calculation and VM execution, the
//!   final commit to some state works on a singleton so the indexed merkle tries may be typically
//!   updated in place. It is also possible (e.g., firewood) to allow some infrequent access to
//!   historical revisions with higher cost, and/or allow fast access to revisions of the store
//!   within certain limited recency. This style of storage is useful for the blockchain systems
//!   where only (or mostly) the latest state is required and data footprint should be minimized if
//!   possible for sustainability. Validators who directly participates in the consensus and vote
//!   for the blocks, for example, can largely benefit from such a design.
//!
//! In firewood, we take a closer look at the second regime and have come up with a simple but
//! solid design that fulfills the need for such blockchain storage.
//!
//! Firewood is built by three layers of abstractions that totally decouple the
//! layout/representation of the data on disk from the actual logical data structure it mains:
//!
//! - Linear, memory-like space: the `shale` crate from some academic project (CedrusDB) code
//!   offers an abstraction for a (64-bit) byte-addressable space that abstracts way the intricate
//!   methods to actually persist the in-memory data on the secondary storage medium (e.g., hard
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
//! ![Three Layers of Firewood](../figures/three-layers.svg)
pub(crate) mod account;
pub mod db;
pub(crate) mod file;
pub mod merkle;
pub(crate) mod storage;
