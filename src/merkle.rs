use enum_as_inner::EnumAsInner;
use parking_lot::{Mutex, MutexGuard};
use shale::{LinearStore, MummyItem, ObjPtr, ObjRef, ShaleError, ShaleStore, WriteContext};
use std::cell::RefCell;

const NBRANCH: usize = 16;

pub enum MerkleError {
    Shale(shale::ShaleError),
}

#[derive(PartialEq, Eq)]
pub struct Hash([u8; 32]);

impl std::ops::Deref for Hash {
    type Target = [u8; 32];
    fn deref(&self) -> &[u8; 32] {
        &self.0
    }
}

impl MummyItem for Hash {
    fn hydrate(addr: u64, mem: &dyn LinearStore) -> Result<(u64, Self), ShaleError> {
        const SIZE: u64 = 32;
        let raw = mem
            .get_ref(addr, SIZE)
            .ok_or(ShaleError::LinearMemStoreError)?;
        Ok((SIZE, Self(raw[..SIZE as usize].try_into().unwrap())))
    }
    fn dehydrate(&self) -> Vec<u8> {
        self.0.to_vec()
    }
}

pub struct MerkleHeader {
    root: ObjPtr<Node>,
}

/// PartialPath keeps a list of nibbles to represent a path on the MPT.
#[derive(PartialEq, Eq)]
struct PartialPath(Vec<u8>);

impl std::ops::Deref for PartialPath {
    type Target = [u8];
    fn deref(&self) -> &[u8] {
        &self.0
    }
}

impl PartialPath {
    fn encode(&self, term: bool) -> Vec<u8> {
        let odd_len = (self.0.len() & 1) as u8;
        let flags = if term { 2 } else { 0 } + odd_len;
        let mut res = if odd_len == 1 {
            vec![flags]
        } else {
            vec![flags, 0x0]
        };
        res.extend(&self.0);
        res
    }

    fn decode(raw: &[u8]) -> (Self, bool) {
        let term = raw[0] > 1;
        let odd_len = raw[0] & 1;
        (
            Self(if odd_len == 1 {
                raw[1..].to_vec()
            } else {
                raw[2..].to_vec()
            }),
            term,
        )
    }
}

#[test]
fn test_partial_path_encoding() {
    let check = |nibbles: &[u8], term| {
        let (d, t) = PartialPath::decode(&PartialPath(nibbles.to_vec()).encode(term));
        assert_eq!(d.0, nibbles);
        assert_eq!(t, term);
    };
    for nibbles in [
        vec![0x1, 0x2, 0x3, 0x4],
        vec![0x1, 0x2, 0x3],
        vec![0x0, 0x1, 0x2],
        vec![0x1, 0x2],
        vec![0x1],
    ] {
        for term in [true, false] {
            check(&nibbles, term)
        }
    }
}

#[derive(PartialEq, Eq, Clone)]
struct Data(Vec<u8>);

impl std::ops::Deref for Data {
    type Target = [u8];
    fn deref(&self) -> &[u8] {
        &self.0
    }
}

#[derive(PartialEq, Eq)]
struct BranchNode {
    chd: [Option<ObjPtr<Node>>; NBRANCH],
    value: Option<Data>,
}

#[derive(PartialEq, Eq)]
struct LeafNode(PartialPath, Data);

#[derive(PartialEq, Eq)]
struct ExtNode(PartialPath, ObjPtr<Node>);

#[derive(PartialEq, Eq)]
struct Node {
    root_hash: Hash,
    inner: NodeType,
}

#[derive(PartialEq, Eq, EnumAsInner)]
enum NodeType {
    Branch(BranchNode),
    Leaf(LeafNode),
    Extension(ExtNode),
}

impl Node {
    fn new(inner: NodeType) -> Self {
        // TODO
        let root_hash = Hash([0; 32]);
        Self { root_hash, inner }
    }
}

impl MummyItem for Node {
    fn hydrate(addr: u64, mem: &dyn LinearStore) -> Result<(u64, Self), ShaleError> {
        let dec_err = |_| ShaleError::DecodeError;
        let meta_raw = mem
            .get_ref(addr, 8 + 32)
            .ok_or(ShaleError::LinearMemStoreError)?;
        let hash = Hash(meta_raw[0..32].try_into().map_err(dec_err)?);
        let len = u64::from_le_bytes(meta_raw[32..].try_into().map_err(dec_err)?);
        let obj_len = 8 + 32 + len;

        // this is different from geth due to the different addressing system, so pointer (8-byte)
        // values are used in place of hashes to refer to a child node.

        let rlp_raw = mem
            .get_ref(addr + 8 + 32, len)
            .ok_or(ShaleError::LinearMemStoreError)?;
        let items: Vec<Vec<u8>> = rlp::decode_list(&rlp_raw);

        if items.len() == NBRANCH + 1 {
            let mut chd = [None; NBRANCH];
            for (i, c) in items[..NBRANCH].iter().enumerate() {
                if c.len() > 0 {
                    chd[i] = Some(unsafe {
                        ObjPtr::new_from_addr(u64::from_le_bytes(
                            c[..8].try_into().map_err(dec_err)?,
                        ))
                    })
                }
            }
            Ok((
                obj_len,
                Self {
                    root_hash: hash,
                    inner: NodeType::Branch(BranchNode {
                        chd,
                        value: if items[NBRANCH].len() > 0 {
                            Some(Data(items[NBRANCH].to_vec()))
                        } else {
                            None
                        },
                    }),
                },
            ))
        } else if items.len() == 2 {
            let (path, term) = PartialPath::decode(&items[0]);
            Ok((
                obj_len,
                if term {
                    Self {
                        root_hash: hash,
                        inner: NodeType::Leaf(LeafNode(path, Data(items[1].clone()))),
                    }
                } else {
                    Self {
                        root_hash: hash,
                        inner: NodeType::Extension(ExtNode(path, unsafe {
                            ObjPtr::new_from_addr(u64::from_le_bytes(
                                items[1][..8].try_into().map_err(dec_err)?,
                            ))
                        })),
                    }
                },
            ))
        } else {
            Err(ShaleError::DecodeError)
        }
    }

    fn dehydrate(&self) -> Vec<u8> {
        let mut items;
        match &self.inner {
            NodeType::Branch(n) => {
                items = Vec::new();
                for c in n.chd.iter() {
                    items.push(match c {
                        Some(p) => p.addr().to_le_bytes().to_vec(),
                        None => Vec::new(),
                    });
                }
                items.push(match &n.value {
                    Some(val) => val.to_vec(),
                    None => Vec::new(),
                });
            }
            NodeType::Extension(n) => {
                items = vec![n.0.encode(false), n.1.addr().to_le_bytes().to_vec()];
            }
            NodeType::Leaf(n) => {
                items = vec![n.0.encode(true), n.1.to_vec()];
            }
        }
        let mut m = Vec::new();
        let rlp_encoded = rlp::encode_list::<Vec<u8>, _>(&items);
        m.extend(self.root_hash.iter());
        m.extend((rlp_encoded.len() as u64).to_le_bytes().to_vec());
        m.extend(rlp_encoded);
        m
    }
}

#[test]
fn test_merkle_node_encoding() {
    let check = |node: Node| {
        let bytes = node.dehydrate();
        let mem = shale::PlainMem::new(bytes.len(), 0x0);
        mem.write(0, &bytes);
        println!("{:?}", bytes);
        let (size, node_) = Node::hydrate(0, &mem).unwrap();
        assert_eq!(bytes.len(), size as usize);
        assert!(node == node_);
    };
    let chd0 = [None; NBRANCH];
    let mut chd1 = chd0;
    for i in 0..NBRANCH / 2 {
        chd1[i] = Some(unsafe { ObjPtr::new_from_addr(0xa) });
    }
    for node in [
        Node {
            root_hash: Hash([0x0; 32]),
            inner: NodeType::Leaf(LeafNode(
                PartialPath(vec![0x1, 0x2, 0x3]),
                Data(vec![0x4, 0x5]),
            )),
        },
        Node {
            root_hash: Hash([0x1; 32]),
            inner: NodeType::Extension(ExtNode(PartialPath(vec![0x1, 0x2, 0x3]), unsafe {
                ObjPtr::new_from_addr(0x42)
            })),
        },
        Node {
            root_hash: Hash([0xf; 32]),
            inner: NodeType::Branch(BranchNode {
                chd: chd0,
                value: Some(Data("hello, world!".as_bytes().to_vec())),
            }),
        },
        Node {
            root_hash: Hash([0xf; 32]),
            inner: NodeType::Branch(BranchNode {
                chd: chd1,
                value: None,
            }),
        },
    ] {
        check(node)
    }
}

pub struct MerkleInner<S: ShaleStore> {
    header: RefCell<ObjRef<'static, MerkleHeader>>,
    store: S,
}

pub struct Merkle<S: ShaleStore>(Mutex<MerkleInner<S>>);

pub struct MerkleBatch<'a, S: ShaleStore> {
    m: MutexGuard<'a, MerkleInner<S>>,
    wctx: WriteContext,
}

impl<S: ShaleStore> Merkle<S> {
    pub fn new(header: ObjRef<'static, MerkleHeader>, store: S) -> Self {
        let header = RefCell::new(header);
        Self(Mutex::new(MerkleInner { header, store }))
    }

    pub fn new_batch(&self) -> MerkleBatch<S> {
        MerkleBatch {
            m: self.0.lock(),
            wctx: WriteContext::new(),
        }
    }
}

fn to_nibbles<'a>(bytes: &'a [u8]) -> impl Iterator<Item = u8> + 'a {
    bytes
        .iter()
        .flat_map(|b| [(b >> 4) & 0xf, b & 0xf].into_iter())
}

#[test]
fn test_to_nibbles() {
    for (bytes, nibbles) in [
        (vec![0x12, 0x34, 0x56], vec![0x1, 0x2, 0x3, 0x4, 0x5, 0x6]),
        (vec![0xc0, 0xff], vec![0xc, 0x0, 0xf, 0xf]),
    ] {
        let n: Vec<_> = to_nibbles(&bytes).collect();
        assert_eq!(n, nibbles);
    }
}

impl<'a, S: ShaleStore> MerkleBatch<'a, S> {
    fn get_node(&self, ptr: ObjPtr<Node>) -> Result<ObjRef<Node>, MerkleError> {
        self.m.store.get_item(ptr).map_err(MerkleError::Shale)
    }
    fn new_node(&self, item: Node) -> Result<ObjRef<Node>, MerkleError> {
        self.m
            .store
            .put_item(item, &self.wctx)
            .map_err(MerkleError::Shale)
    }

    fn split<'b>(
        &self,
        u_ref: &mut ObjRef<'b, Node>,
        parent: Option<&mut (ObjRef<'b, Node>, u8)>,
        rem_path: &[u8],
        n_path: Vec<u8>,
        n_value: Option<Data>,
        val: Vec<u8>,
        deleted: &mut Vec<ObjPtr<Node>>,
        new_root: &mut Option<ObjPtr<Node>>,
    ) -> Result<Option<Vec<u8>>, MerkleError> {
        let u_ptr = u_ref.as_ptr();
        let new_chd = match rem_path.iter().zip(n_path.iter()).position(|(a, b)| a != b) {
            Some(idx) => {
                //                                                     _ [u (new path)]
                //                                                    /
                //  [parent] (-> [ExtNode (common prefix)]) -> [branch]*
                //                                                    \_ [leaf (with val)]
                u_ref.write(
                    |u| {
                        *(match &mut u.inner {
                            NodeType::Leaf(u) => &mut u.0,
                            NodeType::Extension(u) => &mut u.0,
                            _ => unreachable!(),
                        }) = PartialPath(n_path[idx + 1..].to_vec())
                    },
                    true,
                    &self.wctx,
                );
                let leaf = self.new_node(Node::new(NodeType::Leaf(LeafNode(
                    PartialPath(rem_path[idx + 1..].to_vec()),
                    Data(val),
                ))))?;
                let mut chd = [None; NBRANCH];
                chd[rem_path[idx] as usize] = Some(leaf.as_ptr());
                chd[n_path[idx] as usize] = Some(u_ptr);
                let branch =
                    self.new_node(Node::new(NodeType::Branch(BranchNode { chd, value: None })))?;
                if idx > 0 {
                    self.new_node(Node::new(NodeType::Extension(ExtNode(
                        PartialPath(rem_path[..idx].to_vec()),
                        branch.as_ptr(),
                    ))))?
                } else {
                    branch
                }
            }
            None => {
                let (leaf_ptr, prefix, idx, v) = if rem_path.len() < n_path.len() {
                    // key path is a prefix of the path to u
                    u_ref.write(
                        |u| {
                            *(match &mut u.inner {
                                NodeType::Leaf(u) => &mut u.0,
                                NodeType::Extension(u) => &mut u.0,
                                _ => unreachable!(),
                            }) = PartialPath(n_path[rem_path.len() + 1..].to_vec())
                        },
                        true,
                        &self.wctx,
                    );
                    (
                        u_ref.as_ptr(),
                        rem_path,
                        n_path[rem_path.len()],
                        Some(Data(val)),
                    )
                } else {
                    // key path extends the path to u
                    if n_value.is_none() {
                        // this case does not apply to an extension node, resume the tree walk
                        return Ok(Some(val));
                    }
                    let leaf = self.new_node(Node::new(NodeType::Leaf(LeafNode(
                        PartialPath(rem_path[n_path.len() + 1..].to_vec()),
                        Data(val),
                    ))))?;
                    deleted.push(u_ptr);
                    (leaf.as_ptr(), &n_path[..], rem_path[n_path.len()], n_value)
                };
                // [parent] -> [ExtNode] -> [branch with v] -> [Leaf]
                let mut chd = [None; NBRANCH];
                chd[idx as usize] = Some(leaf_ptr);
                let branch =
                    self.new_node(Node::new(NodeType::Branch(BranchNode { chd, value: v })))?;
                self.new_node(Node::new(NodeType::Extension(ExtNode(
                    PartialPath(prefix.to_vec()),
                    branch.as_ptr(),
                ))))?
            }
        }
        .as_ptr();
        // observation: leaf/extension node can only be the child of a branch node
        match parent {
            Some((p_ref, idx)) => p_ref.write(
                |p| p.inner.as_branch_mut().unwrap().chd[*idx as usize] = Some(new_chd),
                true,
                &self.wctx,
            ),
            None => *new_root = Some(new_chd),
        }
        Ok(None)
    }

    pub fn insert(&mut self, key: &[u8], mut val: Vec<u8>) -> Result<(), MerkleError> {
        let mut deleted = Vec::new();
        let mut new_root = None;
        {
            let mut u_ref = self.get_node(self.m.header.borrow().root)?;
            let chunks: Vec<_> = to_nibbles(key).collect();
            let mut parent = None;
            let mut nskip = 0;
            for (i, nib) in chunks.iter().enumerate() {
                if nskip > 0 {
                    nskip -= 1;
                    continue;
                }
                let next_ptr = match &u_ref.inner {
                    NodeType::Branch(n) => match n.chd[*nib as usize] {
                        Some(c) => Some(c),
                        None => {
                            // insert the leaf to the empty slot
                            let leaf = self.new_node(Node::new(NodeType::Leaf(LeafNode(
                                PartialPath(chunks[i + 1..].to_vec()),
                                Data(val),
                            ))))?;
                            u_ref.write(
                                |u| {
                                    u.inner.as_branch_mut().unwrap().chd[*nib as usize] =
                                        Some(leaf.as_ptr());
                                },
                                true,
                                &self.wctx,
                            );
                            break;
                        }
                    },
                    NodeType::Leaf(n) => {
                        let n_path = n.0.to_vec();
                        let n_value = Some(n.1.clone());
                        self.split(
                            &mut u_ref,
                            parent.as_mut(),
                            &chunks[i..],
                            n_path,
                            n_value,
                            val,
                            &mut deleted,
                            &mut new_root,
                        )?;
                        break;
                    }
                    NodeType::Extension(n) => {
                        let n_path = n.0.to_vec();
                        let n_ptr = n.1;
                        nskip = n_path.len() - 1;
                        if let Some(v) = self.split(
                            &mut u_ref,
                            parent.as_mut(),
                            &chunks[i..],
                            n_path,
                            None,
                            val,
                            &mut deleted,
                            &mut new_root,
                        )? {
                            val = v;
                            Some(n_ptr)
                        } else {
                            break;
                        }
                    }
                };
                match next_ptr {
                    Some(next_ptr) => {
                        parent = Some((u_ref, *nib));
                        u_ref = self.get_node(next_ptr)?;
                    }
                    None => {
                        break;
                    }
                }
            }
        }
        if let Some(nr) = new_root {
            self.m
                .header
                .borrow_mut()
                .write(|r| r.root = nr, true, &self.wctx);
        }

        for ptr in deleted.into_iter() {
            self.m
                .store
                .free_item(ptr, &self.wctx)
                .map_err(MerkleError::Shale)?;
        }
        Ok(())
    }
}
