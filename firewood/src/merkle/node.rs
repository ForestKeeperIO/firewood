// Copyright (C) 2023, Ava Labs, Inc. All rights reserved.
// See the file LICENSE.md for licensing terms.

use crate::{
    logger::trace,
    merkle::from_nibbles,
    shale::{disk_address::DiskAddress, CachedStore, ShaleError, ShaleStore, Storable},
};
use bincode::{Error, Options};
use bitflags::bitflags;
use bytemuck::{CheckedBitPattern, NoUninit, Pod, Zeroable};
use enum_as_inner::EnumAsInner;
use serde::{
    de::DeserializeOwned,
    ser::{SerializeSeq, SerializeTuple},
    Deserialize, Serialize,
};
use sha3::{Digest, Keccak256};
use std::{
    fmt::Debug,
    io::{Cursor, Write},
    marker::PhantomData,
    mem::size_of,
    sync::{
        atomic::{AtomicBool, Ordering},
        OnceLock,
    },
};

mod branch;
mod extension;
mod leaf;
mod partial_path;

pub use branch::BranchNode;
pub use extension::ExtNode;
pub use leaf::{LeafNode, SIZE as LEAF_NODE_SIZE};
pub use partial_path::PartialPath;

use crate::nibbles::Nibbles;

use super::{TrieHash, TRIE_HASH_LEN};

bitflags! {
    // should only ever be the size of a nibble
    struct Flags: u8 {
        const TERMINAL = 0b0010;
        const ODD_LEN  = 0b0001;
    }
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct Data(pub(super) Vec<u8>);

impl std::ops::Deref for Data {
    type Target = [u8];
    fn deref(&self) -> &[u8] {
        &self.0
    }
}

impl From<Vec<u8>> for Data {
    fn from(v: Vec<u8>) -> Self {
        Self(v)
    }
}

impl Data {
    pub fn into_inner(self) -> Vec<u8> {
        self.0
    }
}

#[derive(Serialize, Deserialize, Debug)]
enum Encoded<T> {
    Raw(T),
    Data(T),
}

impl Default for Encoded<Vec<u8>> {
    fn default() -> Self {
        // This is the default serialized empty vector
        Encoded::Data(vec![0])
    }
}

impl<T: DeserializeOwned + AsRef<[u8]>> Encoded<T> {
    pub fn decode(self) -> Result<T, bincode::Error> {
        match self {
            Encoded::Raw(raw) => Ok(raw),
            Encoded::Data(data) => bincode::DefaultOptions::new().deserialize(data.as_ref()),
        }
    }
}

#[derive(PartialEq, Eq, Clone, Debug, EnumAsInner)]
pub enum NodeType {
    Branch(Box<BranchNode>),
    Leaf(LeafNode),
    Extension(ExtNode),
}

impl NodeType {
    pub fn decode(buf: &[u8]) -> Result<NodeType, Error> {
        let items: Vec<Encoded<Vec<u8>>> = bincode::DefaultOptions::new().deserialize(buf)?;

        match items.len() {
            LEAF_NODE_SIZE => {
                let mut items = items.into_iter();

                #[allow(clippy::unwrap_used)]
                let decoded_key: Vec<u8> = items.next().unwrap().decode()?;

                let decoded_key_nibbles = Nibbles::<0>::new(&decoded_key);

                let (cur_key_path, term) =
                    PartialPath::from_nibbles(decoded_key_nibbles.into_iter());

                let cur_key = cur_key_path.into_inner();
                #[allow(clippy::unwrap_used)]
                let data: Vec<u8> = items.next().unwrap().decode()?;

                if term {
                    Ok(NodeType::Leaf(LeafNode::new(cur_key, data)))
                } else {
                    Ok(NodeType::Extension(ExtNode {
                        path: PartialPath(cur_key),
                        child: DiskAddress::null(),
                        child_encoded: Some(data),
                    }))
                }
            }
            // TODO: add path
            BranchNode::MSIZE => Ok(NodeType::Branch(BranchNode::decode(buf)?.into())),
            size => Err(Box::new(bincode::ErrorKind::Custom(format!(
                "invalid size: {size}"
            )))),
        }
    }

    pub fn encode<S: ShaleStore<Node>>(&self, store: &S) -> Vec<u8> {
        match &self {
            NodeType::Leaf(n) => n.encode(),
            NodeType::Extension(n) => n.encode(store),
            NodeType::Branch(n) => n.encode(store),
        }
    }

    pub fn path_mut(&mut self) -> &mut PartialPath {
        match self {
            NodeType::Branch(_u) => todo!(),
            NodeType::Leaf(node) => &mut node.path,
            NodeType::Extension(node) => &mut node.path,
        }
    }
}

#[derive(Debug)]
pub struct Node {
    pub(super) root_hash: OnceLock<TrieHash>,
    encoded: OnceLock<Vec<u8>>,
    is_encoded_longer_than_hash_len: OnceLock<bool>,
    // lazy_dirty is an atomicbool, but only writers ever set it
    // Therefore, we can always use Relaxed ordering. It's atomic
    // just to ensure Sync + Send.
    lazy_dirty: AtomicBool,
    pub(super) inner: NodeType,
}

impl Eq for Node {}

impl PartialEq for Node {
    fn eq(&self, other: &Self) -> bool {
        let is_dirty = self.is_dirty();

        let Node {
            root_hash,
            encoded,
            is_encoded_longer_than_hash_len: _,
            lazy_dirty: _,
            inner,
        } = self;
        *root_hash == other.root_hash
            && *encoded == other.encoded
            && is_dirty == other.is_dirty()
            && *inner == other.inner
    }
}

impl Clone for Node {
    fn clone(&self) -> Self {
        Self {
            root_hash: self.root_hash.clone(),
            is_encoded_longer_than_hash_len: self.is_encoded_longer_than_hash_len.clone(),
            encoded: self.encoded.clone(),
            lazy_dirty: AtomicBool::new(self.is_dirty()),
            inner: self.inner.clone(),
        }
    }
}

impl From<NodeType> for Node {
    fn from(inner: NodeType) -> Self {
        let mut s = Self {
            root_hash: OnceLock::new(),
            encoded: OnceLock::new(),
            is_encoded_longer_than_hash_len: OnceLock::new(),
            inner,
            lazy_dirty: AtomicBool::new(false),
        };
        s.rehash();
        s
    }
}

bitflags! {
    #[derive(Debug, Clone, Copy, Pod, Zeroable)]
    #[repr(transparent)]
    struct NodeAttributes: u8 {
        const ROOT_HASH_VALID      = 0b001;
        const IS_ENCODED_BIG_VALID = 0b010;
        const LONG                 = 0b100;
    }
}

impl Node {
    pub(super) fn max_branch_node_size() -> u64 {
        let max_size: OnceLock<u64> = OnceLock::new();
        *max_size.get_or_init(|| {
            Self {
                root_hash: OnceLock::new(),
                encoded: OnceLock::new(),
                is_encoded_longer_than_hash_len: OnceLock::new(),
                inner: NodeType::Branch(
                    BranchNode {
                        // path: vec![].into(),
                        children: [Some(DiskAddress::null()); BranchNode::MAX_CHILDREN],
                        value: Some(Data(Vec::new())),
                        children_encoded: Default::default(),
                    }
                    .into(),
                ),
                lazy_dirty: AtomicBool::new(false),
            }
            .serialized_len()
        })
    }

    pub(super) fn get_encoded<S: ShaleStore<Node>>(&self, store: &S) -> &[u8] {
        self.encoded.get_or_init(|| self.inner.encode::<S>(store))
    }

    pub(super) fn get_root_hash<S: ShaleStore<Node>>(&self, store: &S) -> &TrieHash {
        self.root_hash.get_or_init(|| {
            self.set_dirty(true);
            TrieHash(Keccak256::digest(self.get_encoded::<S>(store)).into())
        })
    }

    fn is_encoded_longer_than_hash_len<S: ShaleStore<Node>>(&self, store: &S) -> bool {
        *self
            .is_encoded_longer_than_hash_len
            .get_or_init(|| self.get_encoded(store).len() >= TRIE_HASH_LEN)
    }

    pub(super) fn rehash(&mut self) {
        self.encoded = OnceLock::new();
        self.is_encoded_longer_than_hash_len = OnceLock::new();
        self.root_hash = OnceLock::new();
    }

    pub fn from_branch<B: Into<Box<BranchNode>>>(node: B) -> Self {
        Self::from(NodeType::Branch(node.into()))
    }

    pub fn from_leaf(leaf: LeafNode) -> Self {
        Self::from(NodeType::Leaf(leaf))
    }

    pub const fn inner(&self) -> &NodeType {
        &self.inner
    }

    pub fn inner_mut(&mut self) -> &mut NodeType {
        &mut self.inner
    }

    pub(super) fn new_from_hash(
        root_hash: Option<TrieHash>,
        encoded: Option<Vec<u8>>,
        is_encoded_longer_than_hash_len: Option<bool>,
        inner: NodeType,
    ) -> Self {
        Self {
            root_hash: match root_hash {
                Some(h) => OnceLock::from(h),
                None => OnceLock::new(),
            },
            encoded: match encoded {
                Some(e) => OnceLock::from(e),
                None => OnceLock::new(),
            },
            is_encoded_longer_than_hash_len: match is_encoded_longer_than_hash_len {
                Some(v) => OnceLock::from(v),
                None => OnceLock::new(),
            },
            inner,
            lazy_dirty: AtomicBool::new(false),
        }
    }

    pub(super) fn is_dirty(&self) -> bool {
        self.lazy_dirty.load(Ordering::Relaxed)
    }

    pub(super) fn set_dirty(&self, is_dirty: bool) {
        self.lazy_dirty.store(is_dirty, Ordering::Relaxed)
    }
}

#[derive(Clone, Copy, CheckedBitPattern, NoUninit)]
#[repr(C, packed)]
struct Meta {
    root_hash: [u8; TRIE_HASH_LEN],
    attrs: NodeAttributes,
    encoded_len: u64,
    encoded: [u8; TRIE_HASH_LEN],
    type_id: NodeTypeId,
}

impl Meta {
    const SIZE: usize = size_of::<Self>();
}

mod type_id {
    use super::{CheckedBitPattern, NoUninit, NodeType};
    use crate::shale::ShaleError;

    #[derive(Clone, Copy, CheckedBitPattern, NoUninit)]
    #[repr(u8)]
    pub enum NodeTypeId {
        Branch = 0,
        Leaf = 1,
        Extension = 2,
    }

    impl TryFrom<u8> for NodeTypeId {
        type Error = ShaleError;

        fn try_from(value: u8) -> Result<Self, Self::Error> {
            bytemuck::checked::try_cast::<_, Self>(value).map_err(|_| ShaleError::InvalidNodeType)
        }
    }

    impl From<&NodeType> for NodeTypeId {
        fn from(node_type: &NodeType) -> Self {
            match node_type {
                NodeType::Branch(_) => NodeTypeId::Branch,
                NodeType::Leaf(_) => NodeTypeId::Leaf,
                NodeType::Extension(_) => NodeTypeId::Extension,
            }
        }
    }
}

use type_id::NodeTypeId;

impl Storable for Node {
    fn deserialize<T: CachedStore>(offset: usize, mem: &T) -> Result<Self, ShaleError> {
        let meta_raw =
            mem.get_view(offset, Meta::SIZE as u64)
                .ok_or(ShaleError::InvalidCacheView {
                    offset,
                    size: Meta::SIZE as u64,
                })?;
        let meta_raw = meta_raw.as_deref();

        let meta = bytemuck::checked::try_from_bytes::<Meta>(&meta_raw)
            .map_err(|_| ShaleError::InvalidNodeMeta)?;

        let Meta {
            root_hash,
            attrs,
            encoded_len,
            encoded,
            type_id,
        } = *meta;

        trace!("[{mem:p}] Deserializing node at {offset}");

        let offset = offset + Meta::SIZE;

        let root_hash = if attrs.contains(NodeAttributes::ROOT_HASH_VALID) {
            Some(TrieHash(root_hash))
        } else {
            None
        };

        let encoded = if encoded_len > 0 {
            Some(encoded.iter().take(encoded_len as usize).copied().collect())
        } else {
            None
        };

        let is_encoded_longer_than_hash_len =
            if attrs.contains(NodeAttributes::IS_ENCODED_BIG_VALID) {
                Some(false)
            } else if attrs.contains(NodeAttributes::LONG) {
                Some(true)
            } else {
                None
            };

        match type_id {
            NodeTypeId::Branch => {
                let inner = NodeType::Branch(Box::new(BranchNode::deserialize(offset, mem)?));

                Ok(Self::new_from_hash(
                    root_hash,
                    encoded,
                    is_encoded_longer_than_hash_len,
                    inner,
                ))
            }

            NodeTypeId::Extension => {
                let inner = NodeType::Extension(ExtNode::deserialize(offset, mem)?);
                let node =
                    Self::new_from_hash(root_hash, encoded, is_encoded_longer_than_hash_len, inner);

                Ok(node)
            }

            NodeTypeId::Leaf => {
                let inner = NodeType::Leaf(LeafNode::deserialize(offset, mem)?);
                let node =
                    Self::new_from_hash(root_hash, encoded, is_encoded_longer_than_hash_len, inner);

                Ok(node)
            }
        }
    }

    fn serialized_len(&self) -> u64 {
        Meta::SIZE as u64
            + match &self.inner {
                NodeType::Branch(n) => n.serialized_len(),
                NodeType::Extension(n) => n.serialized_len(),
                NodeType::Leaf(n) => n.serialized_len(),
            }
    }

    fn serialize(&self, to: &mut [u8]) -> Result<(), ShaleError> {
        trace!("[{self:p}] Serializing node");
        let mut cursor = Cursor::new(to);

        let root_hash = self.root_hash.get().map(|hash| hash.0);
        let encoded = self.encoded.get();
        let encoded_len = encoded.map(Vec::len).map(|len| len as u64);

        let mut attrs = match (encoded_len, self.is_encoded_longer_than_hash_len.get()) {
            // the raw encoded-node is longer than a hash
            (None, Some(true)) => NodeAttributes::LONG,
            (Some(len), _) if len > TRIE_HASH_LEN as u64 => NodeAttributes::LONG,
            //
            (Some(_), _) | (None, Some(false)) => NodeAttributes::IS_ENCODED_BIG_VALID,
            (None, None) => NodeAttributes::empty(),
        };

        if root_hash.is_some() {
            attrs.insert(NodeAttributes::ROOT_HASH_VALID);
        }

        let root_hash = root_hash.unwrap_or_default();

        let (encoded_len, encoded) = {
            let encoded_len = encoded_len.filter(|len| *len <= TRIE_HASH_LEN as u64);
            let encoded = std::array::from_fn({
                let mut iter = encoded_len
                    .and_then(|_| encoded)
                    .map(Vec::as_slice)
                    .unwrap_or_default()
                    .iter()
                    .copied();

                move |_| iter.next().unwrap_or_default()
            });

            (encoded_len.unwrap_or_default(), encoded)
        };

        let type_id = NodeTypeId::from(&self.inner);

        let meta = Meta {
            root_hash,
            attrs,
            encoded_len,
            encoded,
            type_id,
        };

        cursor.write_all(bytemuck::bytes_of(&meta))?;

        match &self.inner {
            NodeType::Branch(n) => {
                let pos = cursor.position() as usize;

                #[allow(clippy::indexing_slicing)]
                n.serialize(&mut cursor.get_mut()[pos..])
            }

            NodeType::Extension(n) => {
                let pos = cursor.position() as usize;

                #[allow(clippy::indexing_slicing)]
                n.serialize(&mut cursor.get_mut()[pos..])
            }

            NodeType::Leaf(n) => {
                let pos = cursor.position() as usize;

                #[allow(clippy::indexing_slicing)]
                n.serialize(&mut cursor.get_mut()[pos..])
            }
        }
    }
}

pub struct EncodedNode<T> {
    pub(crate) node: EncodedNodeType,
    pub(crate) phantom: PhantomData<T>,
}

impl<T> EncodedNode<T> {
    pub const fn new(node: EncodedNodeType) -> Self {
        Self {
            node,
            phantom: PhantomData,
        }
    }
}

pub enum EncodedNodeType {
    Leaf(LeafNode),
    Branch {
        children: Box<[Option<Vec<u8>>; BranchNode::MAX_CHILDREN]>,
        value: Option<Data>,
    },
}

// TODO: probably can merge with `EncodedNodeType`.
#[derive(Debug, Deserialize)]
struct EncodedBranchNode {
    chd: Vec<(u64, Vec<u8>)>,
    data: Option<Vec<u8>>,
    path: Vec<u8>,
}

// Note that the serializer passed in should always be the same type as T in EncodedNode<T>.
impl Serialize for EncodedNode<PlainCodec> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let n = match &self.node {
            EncodedNodeType::Leaf(n) => {
                let data = Some(n.data.to_vec());
                let chd: Vec<(u64, Vec<u8>)> = Default::default();
                let path = from_nibbles(&n.path.encode(true)).collect();
                EncodedBranchNode { chd, data, path }
            }
            EncodedNodeType::Branch { children, value } => {
                let chd: Vec<(u64, Vec<u8>)> = children
                    .iter()
                    .enumerate()
                    .filter_map(|(i, c)| c.as_ref().map(|c| (i as u64, c)))
                    .map(|(i, c)| {
                        if c.len() >= TRIE_HASH_LEN {
                            (i, Keccak256::digest(c).to_vec())
                        } else {
                            (i, c.to_vec())
                        }
                    })
                    .collect();

                let data = value.as_ref().map(|v| v.0.to_vec());
                EncodedBranchNode {
                    chd,
                    data,
                    path: Vec::new(),
                }
            }
        };

        let mut s = serializer.serialize_tuple(3)?;
        s.serialize_element(&n.chd)?;
        s.serialize_element(&n.data)?;
        s.serialize_element(&n.path)?;
        s.end()
    }
}

impl<'de> Deserialize<'de> for EncodedNode<PlainCodec> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let node: EncodedBranchNode = Deserialize::deserialize(deserializer)?;
        if node.chd.is_empty() {
            let data = if let Some(d) = node.data {
                Data(d)
            } else {
                Data(Vec::new())
            };

            let path = PartialPath::from_nibbles(Nibbles::<0>::new(&node.path).into_iter()).0;
            let node = EncodedNodeType::Leaf(LeafNode { path, data });
            Ok(Self::new(node))
        } else {
            let mut children: [Option<Vec<u8>>; BranchNode::MAX_CHILDREN] = Default::default();
            let value = node.data.map(Data);

            for (i, chd) in node.chd {
                #[allow(clippy::indexing_slicing)]
                (children[i as usize] = Some(chd));
            }

            let node = EncodedNodeType::Branch {
                children: children.into(),
                value,
            };
            Ok(Self::new(node))
        }
    }
}

// Note that the serializer passed in should always be the same type as T in EncodedNode<T>.
impl Serialize for EncodedNode<Bincode> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::Error;

        match &self.node {
            EncodedNodeType::Leaf(n) => {
                let list = [
                    Encoded::Raw(from_nibbles(&n.path.encode(true)).collect()),
                    Encoded::Raw(n.data.to_vec()),
                ];
                let mut seq = serializer.serialize_seq(Some(list.len()))?;
                for e in list {
                    seq.serialize_element(&e)?;
                }
                seq.end()
            }
            EncodedNodeType::Branch { children, value } => {
                let mut list = <[Encoded<Vec<u8>>; BranchNode::MAX_CHILDREN + 1]>::default();

                for (i, c) in children
                    .iter()
                    .enumerate()
                    .filter_map(|(i, c)| c.as_ref().map(|c| (i, c)))
                {
                    if c.len() >= TRIE_HASH_LEN {
                        let serialized_hash = Bincode::serialize(&Keccak256::digest(c).to_vec())
                            .map_err(|e| S::Error::custom(format!("bincode error: {e}")))?;
                        #[allow(clippy::indexing_slicing)]
                        (list[i] = Encoded::Data(serialized_hash));
                    } else {
                        #[allow(clippy::indexing_slicing)]
                        (list[i] = Encoded::Raw(c.to_vec()));
                    }
                }
                if let Some(Data(val)) = &value {
                    let serialized_val = Bincode::serialize(val)
                        .map_err(|e| S::Error::custom(format!("bincode error: {e}")))?;
                    list[BranchNode::MAX_CHILDREN] = Encoded::Data(serialized_val);
                }

                let mut seq = serializer.serialize_seq(Some(list.len()))?;
                for e in list {
                    seq.serialize_element(&e)?;
                }
                seq.end()
            }
        }
    }
}

impl<'de> Deserialize<'de> for EncodedNode<Bincode> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        use serde::de::Error;

        let items: Vec<Encoded<Vec<u8>>> = Deserialize::deserialize(deserializer)?;
        let len = items.len();
        match len {
            LEAF_NODE_SIZE => {
                let mut items = items.into_iter();
                let Some(Encoded::Raw(path)) = items.next() else {
                    return Err(D::Error::custom(
                        "incorrect encoded type for leaf node path",
                    ));
                };
                let Some(Encoded::Raw(data)) = items.next() else {
                    return Err(D::Error::custom(
                        "incorrect encoded type for leaf node data",
                    ));
                };
                let path = PartialPath::from_nibbles(Nibbles::<0>::new(&path).into_iter()).0;
                let node = EncodedNodeType::Leaf(LeafNode {
                    path,
                    data: Data(data),
                });
                Ok(Self::new(node))
            }
            BranchNode::MSIZE => {
                let mut children: [Option<Vec<u8>>; BranchNode::MAX_CHILDREN] = Default::default();
                let mut value: Option<Data> = Default::default();
                let len = items.len();

                for (i, chd) in items.into_iter().enumerate() {
                    if i == len - 1 {
                        let data = match chd {
                            Encoded::Raw(data) => Err(D::Error::custom(format!(
                                "incorrect encoded type for branch node value {:?}",
                                data
                            )))?,
                            Encoded::Data(data) => Bincode::deserialize(data.as_ref())
                                .map_err(|e| D::Error::custom(format!("bincode error: {e}")))?,
                        };
                        // Extract the value of the branch node and set to None if it's an empty Vec
                        value = Some(Data(data)).filter(|data| !data.is_empty());
                    } else {
                        let chd = match chd {
                            Encoded::Raw(chd) => chd,
                            Encoded::Data(chd) => Bincode::deserialize(chd.as_ref())
                                .map_err(|e| D::Error::custom(format!("bincode error: {e}")))?,
                        };
                        #[allow(clippy::indexing_slicing)]
                        (children[i] = Some(chd).filter(|chd| !chd.is_empty()));
                    }
                }
                let node = EncodedNodeType::Branch {
                    children: children.into(),
                    value,
                };
                Ok(Self::new(node))
            }
            size => Err(D::Error::custom(format!("invalid size: {size}"))),
        }
    }
}

pub trait BinarySerde {
    type SerializeError: serde::ser::Error;
    type DeserializeError: serde::de::Error;

    fn new() -> Self;

    fn serialize<T: Serialize>(t: &T) -> Result<Vec<u8>, Self::SerializeError>
    where
        Self: Sized,
    {
        Self::new().serialize_impl(t)
    }

    fn deserialize<'de, T: Deserialize<'de>>(bytes: &'de [u8]) -> Result<T, Self::DeserializeError>
    where
        Self: Sized,
    {
        Self::new().deserialize_impl(bytes)
    }

    fn serialize_impl<T: Serialize>(&self, t: &T) -> Result<Vec<u8>, Self::SerializeError>;
    fn deserialize_impl<'de, T: Deserialize<'de>>(
        &self,
        bytes: &'de [u8],
    ) -> Result<T, Self::DeserializeError>;
}

#[derive(Default)]
pub struct Bincode(pub bincode::DefaultOptions);

impl Debug for Bincode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        write!(f, "[bincode::DefaultOptions]")
    }
}

impl BinarySerde for Bincode {
    type SerializeError = bincode::Error;
    type DeserializeError = Self::SerializeError;

    fn new() -> Self {
        Self(bincode::DefaultOptions::new())
    }

    fn serialize_impl<T: Serialize>(&self, t: &T) -> Result<Vec<u8>, Self::SerializeError> {
        self.0.serialize(t)
    }

    fn deserialize_impl<'de, T: Deserialize<'de>>(
        &self,
        bytes: &'de [u8],
    ) -> Result<T, Self::DeserializeError> {
        self.0.deserialize(bytes)
    }
}

#[derive(Default)]
pub struct PlainCodec(pub bincode::DefaultOptions);

impl Debug for PlainCodec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        write!(f, "PlainCodec")
    }
}

impl BinarySerde for PlainCodec {
    type SerializeError = bincode::Error;
    type DeserializeError = Self::SerializeError;

    fn new() -> Self {
        Self(bincode::DefaultOptions::new())
    }

    fn serialize_impl<T: Serialize>(&self, t: &T) -> Result<Vec<u8>, Self::SerializeError> {
        // Serializes the object directly into a Writer without include the length.
        let mut writer = Vec::new();
        self.0.serialize_into(&mut writer, t)?;
        Ok(writer)
    }

    fn deserialize_impl<'de, T: Deserialize<'de>>(
        &self,
        bytes: &'de [u8],
    ) -> Result<T, Self::DeserializeError> {
        self.0.deserialize(bytes)
    }
}

#[cfg(test)]
pub(super) mod tests {
    use std::array::from_fn;

    use super::*;
    use crate::shale::cached::PlainMem;
    use test_case::test_case;

    pub fn leaf(path: Vec<u8>, data: Vec<u8>) -> Node {
        Node::from_leaf(LeafNode::new(PartialPath(path), Data(data)))
    }

    pub fn leaf_from_hash(
        root_hash: Option<TrieHash>,
        encoded: Option<Vec<u8>>,
        is_encoded_longer_than_hash_len: Option<bool>,
        path: Vec<u8>,
        data: Vec<u8>,
    ) -> Node {
        let inner = NodeType::Leaf(LeafNode::new(PartialPath(path), Data(data)));
        Node::new_from_hash(root_hash, encoded, is_encoded_longer_than_hash_len, inner)
    }

    fn new_branch(
        repeated_disk_address: usize,
        value: Option<Vec<u8>>,
        repeated_encoded_child: Option<Vec<u8>>,
    ) -> BranchNode {
        let children: [Option<DiskAddress>; BranchNode::MAX_CHILDREN] = from_fn(|i| {
            if i < BranchNode::MAX_CHILDREN / 2 {
                DiskAddress::from(repeated_disk_address).into()
            } else {
                None
            }
        });

        let children_encoded = repeated_encoded_child
            .map(|child| {
                from_fn(|i| {
                    if i < BranchNode::MAX_CHILDREN / 2 {
                        child.clone().into()
                    } else {
                        None
                    }
                })
            })
            .unwrap_or_default();

        BranchNode {
            // path: vec![].into(),
            children,
            value: value.map(Data),
            children_encoded,
        }
    }

    pub fn branch(
        repeated_disk_address: usize,
        value: Option<Vec<u8>>,
        repeated_encoded_child: Option<Vec<u8>>,
    ) -> Node {
        Node::from_branch(new_branch(
            repeated_disk_address,
            value,
            repeated_encoded_child,
        ))
    }

    pub fn branch_with_hash(
        root_hash: Option<TrieHash>,
        encoded: Option<Vec<u8>>,
        is_encoded_longer_than_hash_len: Option<bool>,
        repeated_disk_address: usize,
        value: Option<Vec<u8>>,
        repeated_encoded_child: Option<Vec<u8>>,
    ) -> Node {
        Node::new_from_hash(
            root_hash,
            encoded,
            is_encoded_longer_than_hash_len,
            NodeType::Branch(
                new_branch(repeated_disk_address, value, repeated_encoded_child).into(),
            ),
        )
    }

    pub fn extension(
        path: Vec<u8>,
        child_address: DiskAddress,
        child_encoded: Option<Vec<u8>>,
    ) -> Node {
        Node::from(NodeType::Extension(ExtNode {
            path: PartialPath(path),
            child: child_address,
            child_encoded,
        }))
    }

    pub fn extension_from_hash(
        root_hash: Option<TrieHash>,
        encoded: Option<Vec<u8>>,
        is_encoded_longer_than_hash_len: Option<bool>,
        path: Vec<u8>,
        child_address: DiskAddress,
        child_encoded: Option<Vec<u8>>,
    ) -> Node {
        let inner = NodeType::Extension(ExtNode {
            path: PartialPath(path),
            child: child_address,
            child_encoded,
        });
        Node::new_from_hash(root_hash, encoded, is_encoded_longer_than_hash_len, inner)
    }

    #[test_case(leaf(vec![0x01, 0x02, 0x03], vec![0x04, 0x05]); "leaf node")]
    #[test_case(leaf_from_hash(None, Some(vec![0x01, 0x02, 0x03]), Some(false), vec![0x01, 0x02, 0x03], vec![0x04, 0x05]); "leaf node with encoded")]
    #[test_case(leaf_from_hash(Some(TrieHash([0x01; 32])), None, Some(true), vec![0x01, 0x02, 0x03], vec![0x04, 0x05]); "leaf node with hash")]
    #[test_case(extension(vec![0x01, 0x02, 0x03], DiskAddress::from(0x42), None); "extension with child address")]
    #[test_case(extension(vec![0x01, 0x02, 0x03], DiskAddress::null(), vec![0x01, 0x02, 0x03].into()) ; "extension without child address")]
    #[test_case(extension_from_hash(None, Some(vec![0x01, 0x02, 0x03]), Some(false), vec![0x01, 0x02, 0x03], DiskAddress::from(0x42), None); "extension with encoded")]
    #[test_case(extension_from_hash(Some(TrieHash([0x01; 32])), None, Some(true), vec![0x01, 0x02, 0x03], DiskAddress::from(0x42), None); "extension with hash")]
    #[test_case(branch(0x0a, b"hello world".to_vec().into(), None); "branch with data")]
    #[test_case(branch(0x0a, None, vec![0x01, 0x02, 0x03].into()); "branch without data")]
    #[test_case(branch_with_hash(Some(TrieHash([0x01; 32])), None, Some(true), 0x0a, b"hello world".to_vec().into(), None); "branch with hash value")]
    #[test_case(branch_with_hash(None, Some(vec![0x01, 0x02, 0x03]), Some(false), 0x0a, b"hello world".to_vec().into(), None); "branch with encoded")]
    fn test_encoding(node: Node) {
        let mut bytes = vec![0; node.serialized_len() as usize];

        #[allow(clippy::unwrap_used)]
        node.serialize(&mut bytes).unwrap();

        let mut mem = PlainMem::new(node.serialized_len(), 0x00);
        mem.write(0, &bytes);

        #[allow(clippy::unwrap_used)]
        let hydrated_node = Node::deserialize(0, &mem).unwrap();

        assert_eq!(node, hydrated_node);
    }
}
