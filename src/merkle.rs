use enum_as_inner::EnumAsInner;
use sha3::Digest;
use shale::{MemStore, MummyItem, ObjPtr, ObjRef, ShaleError, ShaleStore};

use std::cell::RefCell;
use std::fmt::Debug;

const NBRANCH: usize = 16;

#[derive(Debug)]
pub enum MerkleError {
    Shale(shale::ShaleError),
}

#[derive(PartialEq, Eq, Clone)]
pub struct Hash([u8; 32]);

impl std::ops::Deref for Hash {
    type Target = [u8; 32];
    fn deref(&self) -> &[u8; 32] {
        &self.0
    }
}

impl MummyItem for Hash {
    fn hydrate(addr: u64, mem: &dyn MemStore) -> Result<(u64, Self), ShaleError> {
        const SIZE: u64 = 32;
        let raw = mem.get_ref(addr, SIZE).ok_or(ShaleError::LinearMemStoreError)?;
        Ok((SIZE, Self(raw[..SIZE as usize].try_into().unwrap())))
    }
    fn dehydrate(&self) -> Vec<u8> {
        self.0.to_vec()
    }
}

pub struct MerkleHeader {
    root: ObjPtr<Node>,
}

impl MerkleHeader {
    pub const MSIZE: u64 = 8;

    pub fn new_empty() -> Self {
        Self { root: ObjPtr::null() }
    }
}

impl MummyItem for MerkleHeader {
    fn hydrate(addr: u64, mem: &dyn MemStore) -> Result<(u64, Self), ShaleError> {
        let raw = mem.get_ref(addr, Self::MSIZE).ok_or(ShaleError::LinearMemStoreError)?;
        let root = u64::from_le_bytes(raw[..8].try_into().unwrap());
        unsafe {
            Ok((
                Self::MSIZE,
                Self {
                    root: ObjPtr::new_from_addr(root),
                },
            ))
        }
    }

    fn dehydrate(&self) -> Vec<u8> {
        let mut m = Vec::new();
        m.extend(self.root.addr().to_le_bytes());
        assert_eq!(m.len() as u64, Self::MSIZE);
        m
    }
}

/// PartialPath keeps a list of nibbles to represent a path on the MPT.
#[derive(PartialEq, Eq, Clone)]
struct PartialPath(Vec<u8>);

impl Debug for PartialPath {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        for nib in self.0.iter() {
            write!(f, "{:x}", *nib & 0xf)?;
        }
        Ok(())
    }
}

impl std::ops::Deref for PartialPath {
    type Target = [u8];
    fn deref(&self) -> &[u8] {
        &self.0
    }
}

impl PartialPath {
    fn into_inner(self) -> Vec<u8> {
        self.0
    }

    fn encode(&self, term: bool) -> Vec<u8> {
        let odd_len = (self.0.len() & 1) as u8;
        let flags = if term { 2 } else { 0 } + odd_len;
        let mut res = if odd_len == 1 { vec![flags] } else { vec![flags, 0x0] };
        res.extend(&self.0);
        res
    }

    fn decode<R: AsRef<[u8]>>(raw: R) -> (Self, bool) {
        let raw = raw.as_ref();
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
    let check = |steps: &[u8], term| {
        let (d, t) = PartialPath::decode(&PartialPath(steps.to_vec()).encode(term));
        assert_eq!(d.0, steps);
        assert_eq!(t, term);
    };
    for steps in [
        vec![0x1, 0x2, 0x3, 0x4],
        vec![0x1, 0x2, 0x3],
        vec![0x0, 0x1, 0x2],
        vec![0x1, 0x2],
        vec![0x1],
    ] {
        for term in [true, false] {
            check(&steps, term)
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

#[derive(PartialEq, Eq, Clone)]
struct BranchNode {
    chd: [Option<ObjPtr<Node>>; NBRANCH],
    value: Option<Data>,
}

impl Debug for BranchNode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        write!(f, "[Branch")?;
        for (i, c) in self.chd.iter().enumerate() {
            if let Some(c) = c {
                write!(f, " ({:x} {})", i, c)?;
            }
        }
        write!(
            f,
            " v={}]",
            match &self.value {
                Some(v) => hex::encode(&**v),
                None => "nil".to_string(),
            }
        )
    }
}

impl BranchNode {
    fn single_child(&self) -> (Option<(ObjPtr<Node>, u8)>, bool) {
        let mut has_chd = false;
        let mut only_chd = None;
        for (i, c) in self.chd.iter().enumerate() {
            if c.is_some() {
                has_chd = true;
                if only_chd.is_some() {
                    only_chd = None;
                    break
                }
                only_chd = c.clone().map(|e| (e, i as u8))
            }
        }
        (only_chd, has_chd)
    }

    fn rlp<S: ShaleStore>(&self, store: &S) -> Vec<u8> {
        let mut stream = rlp::RlpStream::new_list(17);
        for c in self.chd.iter() {
            match c {
                Some(c) => {
                    let c_ref = store.get_item(*c).unwrap();
                    let c_rlp = c_ref.inner.rlp(store);
                    if c_rlp.len() >= 32 {
                        stream.append(&sha3::Keccak256::digest(c_rlp).to_vec())
                    } else {
                        stream.append_raw(&c_rlp, 1)
                    }
                }
                None => stream.append_empty_data(),
            };
        }
        match &self.value {
            Some(val) => stream.append(&val.to_vec()),
            None => stream.append_empty_data(),
        };
        stream.out().into()
    }
}

#[derive(PartialEq, Eq, Clone)]
struct LeafNode(PartialPath, Data);

impl Debug for LeafNode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        write!(f, "[Leaf {:?} {}]", self.0, hex::encode(&*self.1))
    }
}

impl LeafNode {
    fn rlp(&self) -> Vec<u8> {
        rlp::encode_list::<Vec<u8>, _>(&[from_nibbles(&self.0.encode(true)).collect(), self.1.to_vec()]).into()
    }
}

#[derive(PartialEq, Eq, Clone)]
struct ExtNode(PartialPath, ObjPtr<Node>);

impl Debug for ExtNode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        write!(f, "[Extension {:?} {}]", self.0, self.1)
    }
}

impl ExtNode {
    fn rlp<S: ShaleStore>(&self, store: &S) -> Vec<u8> {
        let r = store.get_item(self.1).unwrap();
        let rlp = r.inner.rlp(store);
        let mut stream = rlp::RlpStream::new_list(2);
        stream.append(&from_nibbles(&self.0.encode(false)).collect::<Vec<_>>());
        if rlp.len() >= 32 {
            stream.append(&sha3::Keccak256::digest(rlp).to_vec());
        } else {
            stream.append_raw(&rlp, 1);
        }
        stream.out().into()
    }
}

#[derive(PartialEq, Eq, Clone)]
struct Node {
    root_hash: Hash,
    inner: NodeType,
}

#[derive(PartialEq, Eq, Clone, EnumAsInner)]
enum NodeType {
    Branch(BranchNode),
    Leaf(LeafNode),
    Extension(ExtNode),
}

impl NodeType {
    fn rlp<S: ShaleStore>(&self, store: &S) -> Vec<u8> {
        match &self {
            NodeType::Leaf(n) => n.rlp(),
            NodeType::Extension(n) => n.rlp(store),
            NodeType::Branch(n) => n.rlp(store),
        }
    }

    fn hash<S: ShaleStore>(&self, store: &S) -> Hash {
        Hash(sha3::Keccak256::digest(self.rlp(store)).into())
    }
}

impl Node {
    fn new<S: ShaleStore>(inner: NodeType, store: &S) -> Self {
        let root_hash = inner.hash(store);
        Self { root_hash, inner }
    }
}

impl MummyItem for Node {
    fn hydrate(addr: u64, mem: &dyn MemStore) -> Result<(u64, Self), ShaleError> {
        let dec_err = |_| ShaleError::DecodeError;
        let meta_raw = mem.get_ref(addr, 8 + 32).ok_or(ShaleError::LinearMemStoreError)?;
        let hash = Hash(meta_raw[0..32].try_into().map_err(dec_err)?);
        let len = u64::from_le_bytes(meta_raw[32..].try_into().map_err(dec_err)?);
        let obj_len = 8 + 32 + len;

        // this is different from geth due to the different addressing system, so pointer (8-byte)
        // values are used in place of hashes to refer to a child node.

        let rlp_raw = mem.get_ref(addr + 8 + 32, len).ok_or(ShaleError::LinearMemStoreError)?;
        let items: Vec<Vec<u8>> = rlp::decode_list(&rlp_raw);

        if items.len() == NBRANCH + 1 {
            let mut chd = [None; NBRANCH];
            for (i, c) in items[..NBRANCH].iter().enumerate() {
                if c.len() > 0 {
                    chd[i] =
                        Some(unsafe { ObjPtr::new_from_addr(u64::from_le_bytes(c[..8].try_into().map_err(dec_err)?)) })
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
            let nibbles: Vec<_> = to_nibbles(&items[0]).collect();
            let (path, term) = PartialPath::decode(nibbles);
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
                            ObjPtr::new_from_addr(u64::from_le_bytes(items[1][..8].try_into().map_err(dec_err)?))
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
                items = vec![
                    from_nibbles(&n.0.encode(false)).collect(),
                    n.1.addr().to_le_bytes().to_vec(),
                ];
            }
            NodeType::Leaf(n) => {
                items = vec![from_nibbles(&n.0.encode(true)).collect(), n.1.to_vec()];
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
        let mem = shale::PlainMem::new(bytes.len() as u64, 0x0);
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
            inner: NodeType::Leaf(LeafNode(PartialPath(vec![0x1, 0x2, 0x3]), Data(vec![0x4, 0x5]))),
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
            inner: NodeType::Branch(BranchNode { chd: chd1, value: None }),
        },
    ] {
        check(node)
    }
}

macro_rules! write_node {
    ($self: ident, $r: expr, $modify: expr, $parents: expr, $deleted: expr) => {
        if let None = $r.write($modify) {
            $self.set_parent($self.new_node($r.clone())?.as_ptr(), $parents);
            $deleted.push($r.as_ptr());
            true
        } else {
            false
        }
    };
}

pub struct Merkle<S: ShaleStore> {
    header: RefCell<ObjRef<'static, MerkleHeader>>,
    store: S,
}

impl<S: ShaleStore> Merkle<S> {
    fn get_node(&self, ptr: ObjPtr<Node>) -> Result<ObjRef<Node>, MerkleError> {
        self.store.get_item(ptr).map_err(MerkleError::Shale)
    }
}

impl<S: ShaleStore> Merkle<S> {
    pub fn new(mut header: ObjRef<'static, MerkleHeader>, store: S) -> Self {
        if header.root.is_null() {
            // create the sentinel node
            header
                .write(|r| {
                    r.root = store
                        .put_item(
                            Node::new(
                                NodeType::Branch(BranchNode {
                                    chd: [None; NBRANCH],
                                    value: None,
                                }),
                                &store,
                            ),
                            256,
                        )
                        .unwrap()
                        .as_ptr()
                })
                .unwrap();
        }
        let header = RefCell::new(header);
        Self { header, store }
    }

    fn empty_root() -> &'static Hash {
        use once_cell::sync::OnceCell;
        static V: OnceCell<Hash> = OnceCell::new();
        V.get_or_init(|| {
            Hash(
                hex::decode("56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421")
                    .unwrap()
                    .try_into()
                    .unwrap(),
            )
        })
    }

    pub fn root_hash(&self) -> Hash {
        let root = self
            .get_node(self.header.borrow().root)
            .unwrap()
            .inner
            .as_branch()
            .unwrap()
            .chd[0];
        if let Some(root) = root {
            self.get_node(root).unwrap().root_hash.clone()
        } else {
            Self::empty_root().clone()
        }
    }

    fn dump_(&self, u: ObjPtr<Node>, output: &mut String) {
        use std::fmt::Write;
        let u_ref = self.get_node(u).unwrap();
        write!(output, "{} => {}: ", u, hex::encode(&*u_ref.root_hash)).unwrap();
        match &u_ref.inner {
            NodeType::Branch(n) => {
                writeln!(output, "{:?}", n).unwrap();
                for c in n.chd.iter() {
                    if let Some(c) = c {
                        self.dump_(*c, output)
                    }
                }
            }
            NodeType::Leaf(n) => writeln!(output, "{:?}", n).unwrap(),
            NodeType::Extension(n) => {
                writeln!(output, "{:?}", n).unwrap();
                self.dump_(n.1, output)
            }
        }
    }

    pub fn dump(&self) -> String {
        let root = self.header.borrow().root;
        if root.is_null() {
            "<Empty>".to_string()
        } else {
            let mut s = String::new();
            self.dump_(root, &mut s);
            s
        }
    }

    fn new_node(&self, item: Node) -> Result<ObjRef<Node>, MerkleError> {
        self.store.put_item(item, 0).map_err(MerkleError::Shale)
    }

    fn set_parent<'b>(&self, new_chd: ObjPtr<Node>, parents: &mut [(ObjRef<'b, Node>, u8)]) {
        let (p_ref, idx) = parents.last_mut().unwrap();
        p_ref
            .write(|p| {
                match &mut p.inner {
                    NodeType::Branch(pp) => pp.chd[*idx as usize] = Some(new_chd),
                    NodeType::Extension(pp) => pp.1 = new_chd,
                    _ => unreachable!(),
                }
                p.root_hash = p.inner.hash(&self.store);
            })
            .unwrap();
    }

    fn split<'b>(
        &self, u_ref: &mut ObjRef<'b, Node>, parents: &mut [(ObjRef<'b, Node>, u8)], rem_path: &[u8], n_path: Vec<u8>,
        n_value: Option<Data>, val: Vec<u8>, deleted: &mut Vec<ObjPtr<Node>>,
    ) -> Result<Option<Vec<u8>>, MerkleError> {
        let u_ptr = u_ref.as_ptr();
        let new_chd = match rem_path.iter().zip(n_path.iter()).position(|(a, b)| a != b) {
            Some(idx) => {
                //                                                      _ [u (new path)]
                //                                                     /
                //  [parent] (-> [ExtNode (common prefix)]) -> [branch]*
                //                                                     \_ [leaf (with val)]
                u_ref
                    .write(|u| {
                        (*match &mut u.inner {
                            NodeType::Leaf(u) => &mut u.0,
                            NodeType::Extension(u) => &mut u.0,
                            _ => unreachable!(),
                        }) = PartialPath(n_path[idx + 1..].to_vec());
                        u.root_hash = u.inner.hash(&self.store);
                    })
                    .unwrap();
                let leaf = self.new_node(Node::new(
                    NodeType::Leaf(LeafNode(PartialPath(rem_path[idx + 1..].to_vec()), Data(val))),
                    &self.store,
                ))?;
                let mut chd = [None; NBRANCH];
                chd[rem_path[idx] as usize] = Some(leaf.as_ptr());
                chd[n_path[idx] as usize] = Some(match &u_ref.inner {
                    NodeType::Extension(u) => {
                        if u.0.len() == 0 {
                            deleted.push(u_ptr);
                            u.1
                        } else {
                            u_ptr
                        }
                    }
                    _ => u_ptr,
                });
                let branch = self.new_node(Node::new(
                    NodeType::Branch(BranchNode { chd, value: None }),
                    &self.store,
                ))?;
                if idx > 0 {
                    self.new_node(Node::new(
                        NodeType::Extension(ExtNode(PartialPath(rem_path[..idx].to_vec()), branch.as_ptr())),
                        &self.store,
                    ))?
                } else {
                    branch
                }
            }
            None => {
                if rem_path.len() == n_path.len() {
                    u_ref
                        .write(|u| {
                            match &mut u.inner {
                                NodeType::Leaf(u) => u.1 = Data(val),
                                NodeType::Extension(u) => {
                                    let mut b_ref = self.get_node(u.1).unwrap();
                                    if let None =
                                        b_ref.write(|b| b.inner.as_branch_mut().unwrap().value = Some(Data(val)))
                                    {
                                        u.1 = self.new_node(b_ref.clone()).unwrap().as_ptr();
                                        deleted.push(b_ref.as_ptr());
                                    }
                                }
                                _ => unreachable!(),
                            }
                            u.root_hash = u.inner.hash(&self.store);
                        })
                        .unwrap();
                    return Ok(None)
                }
                let (leaf_ptr, prefix, idx, v) = if rem_path.len() < n_path.len() {
                    // key path is a prefix of the path to u
                    u_ref
                        .write(|u| {
                            (*match &mut u.inner {
                                NodeType::Leaf(u) => &mut u.0,
                                NodeType::Extension(u) => &mut u.0,
                                _ => unreachable!(),
                            }) = PartialPath(n_path[rem_path.len() + 1..].to_vec());
                            u.root_hash = u.inner.hash(&self.store);
                        })
                        .unwrap();
                    (
                        match &u_ref.inner {
                            NodeType::Extension(u) => {
                                if u.0.len() == 0 {
                                    deleted.push(u_ptr);
                                    u.1
                                } else {
                                    u_ptr
                                }
                            }
                            _ => u_ptr,
                        },
                        rem_path,
                        n_path[rem_path.len()],
                        Some(Data(val)),
                    )
                } else {
                    // key path extends the path to u
                    if n_value.is_none() {
                        // this case does not apply to an extension node, resume the tree walk
                        return Ok(Some(val))
                    }
                    let leaf = self.new_node(Node::new(
                        NodeType::Leaf(LeafNode(PartialPath(rem_path[n_path.len() + 1..].to_vec()), Data(val))),
                        &self.store,
                    ))?;
                    deleted.push(u_ptr);
                    (leaf.as_ptr(), &n_path[..], rem_path[n_path.len()], n_value)
                };
                // [parent] (-> [ExtNode]) -> [branch with v] -> [Leaf]
                let mut chd = [None; NBRANCH];
                chd[idx as usize] = Some(leaf_ptr);
                let branch = self.new_node(Node::new(NodeType::Branch(BranchNode { chd, value: v }), &self.store))?;
                if prefix.len() > 0 {
                    self.new_node(Node::new(
                        NodeType::Extension(ExtNode(PartialPath(prefix.to_vec()), branch.as_ptr())),
                        &self.store,
                    ))?
                } else {
                    branch
                }
            }
        }
        .as_ptr();
        // observation:
        // - leaf/extension node can only be the child of a branch node
        // - branch node can only be the child of a branch/extension node
        self.set_parent(new_chd, parents);
        Ok(None)
    }

    pub fn insert<K: AsRef<[u8]>>(&mut self, key: K, val: Vec<u8>) -> Result<(), MerkleError> {
        let mut deleted = Vec::new();
        let mut updated = Vec::new();
        let root = self.header.borrow().root;
        let mut chunks = vec![0];
        chunks.extend(to_nibbles(key.as_ref()));
        let mut u_ref = self.get_node(root)?;
        let mut parents = Vec::new();
        let mut nskip = 0;
        let mut val = Some(val);
        for (i, nib) in chunks.iter().enumerate() {
            if nskip > 0 {
                nskip -= 1;
                continue
            }
            let next_ptr = match &u_ref.inner {
                NodeType::Branch(n) => match n.chd[*nib as usize] {
                    Some(c) => c,
                    None => {
                        // insert the leaf to the empty slot
                        let leaf = self.new_node(Node::new(
                            NodeType::Leaf(LeafNode(
                                PartialPath(chunks[i + 1..].to_vec()),
                                Data(val.take().unwrap()),
                            )),
                            &self.store,
                        ))?;
                        write_node!(
                            self,
                            &mut u_ref,
                            |u| {
                                let uu = u.inner.as_branch_mut().unwrap();
                                uu.chd[*nib as usize] = Some(leaf.as_ptr());
                                u.root_hash = u.inner.hash(&self.store);
                            },
                            &mut parents,
                            &mut deleted
                        );
                        break
                    }
                },
                NodeType::Leaf(n) => {
                    let n_path = n.0.to_vec();
                    let n_value = Some(n.1.clone());
                    self.split(
                        &mut u_ref,
                        &mut parents,
                        &chunks[i..],
                        n_path,
                        n_value,
                        val.take().unwrap(),
                        &mut deleted,
                    )?;
                    break
                }
                NodeType::Extension(n) => {
                    let n_path = n.0.to_vec();
                    let n_ptr = n.1;
                    nskip = n_path.len() - 1;
                    if let Some(v) = self.split(
                        &mut u_ref,
                        &mut parents,
                        &chunks[i..],
                        n_path,
                        None,
                        val.take().unwrap(),
                        &mut deleted,
                    )? {
                        val = Some(v);
                        n_ptr
                    } else {
                        break
                    }
                }
            };

            updated.push(u_ref.as_ptr());
            parents.push((u_ref, *nib));
            u_ref = self.get_node(next_ptr)?;
        }
        if val.is_some() {
            let u_ptr = u_ref.as_ptr();
            write_node!(
                self,
                &mut u_ref,
                |u| {
                    if let Some((path, ext)) = match &mut u.inner {
                        NodeType::Branch(n) => {
                            n.value = Some(Data(val.take().unwrap()));
                            None
                        }
                        NodeType::Leaf(n) => {
                            if n.0.len() == 0 {
                                n.1 = Data(val.take().unwrap());
                                None
                            } else {
                                Some((&mut n.0, None))
                            }
                        }
                        NodeType::Extension(n) => Some((&mut n.0, Some(n.1))),
                    } {
                        let mut chd = [None; NBRANCH];
                        let idx = path[0] as usize;
                        let c_ptr = if path.len() > 1 || ext.is_none() {
                            *path = PartialPath(path[1..].to_vec());
                            u_ptr
                        } else {
                            deleted.push(u_ptr);
                            ext.unwrap()
                        };
                        chd[idx] = Some(c_ptr);
                        let branch = self
                            .new_node(Node::new(
                                NodeType::Branch(BranchNode {
                                    chd,
                                    value: Some(Data(val.take().unwrap())),
                                }),
                                &self.store,
                            ))
                            .unwrap()
                            .as_ptr();
                        self.set_parent(branch, &mut parents);
                    }
                    u.root_hash = u.inner.hash(&self.store);
                },
                &mut parents,
                &mut deleted
            );
        }

        for ptr in updated.into_iter() {
            self.get_node(ptr)
                .unwrap()
                .write(|u| u.root_hash = u.inner.hash(&self.store))
                .unwrap();
        }

        drop(u_ref);
        drop(parents);
        for ptr in deleted.into_iter() {
            self.store.free_item(ptr).map_err(MerkleError::Shale)?;
        }
        Ok(())
    }

    fn after_remove_leaf<'b>(
        &self, parents: &mut Vec<(ObjRef<'b, Node>, u8)>, deleted: &mut Vec<ObjPtr<Node>>,
    ) -> Result<(), MerkleError> {
        let (mut b_ref, b_idx) = parents.pop().unwrap();
        // the immediate parent of a leaf must be a branch
        b_ref
            .write(|b| b.inner.as_branch_mut().unwrap().chd[b_idx as usize] = None)
            .unwrap();
        let b_inner = b_ref.inner.as_branch().unwrap();
        let (b_chd, has_chd) = b_inner.single_child();
        if (has_chd && (b_chd.is_none() || b_inner.value.is_some())) || parents.len() < 1 {
            return Ok(())
        }
        deleted.push(b_ref.as_ptr());
        let val = b_inner.value.clone();
        let (mut p_ref, p_idx) = parents.pop().unwrap();
        let p_ptr = p_ref.as_ptr();
        if let Some(val) = val {
            match &p_ref.inner {
                NodeType::Branch(_) => {
                    // from: [p: Branch] -> [b (v)]x -> [Leaf]x
                    // to: [p: Branch] -> [Leaf (v)]
                    let leaf = self
                        .new_node(Node::new(
                            NodeType::Leaf(LeafNode(PartialPath(Vec::new()), val)),
                            &self.store,
                        ))
                        .unwrap()
                        .as_ptr();
                    write_node!(
                        self,
                        p_ref,
                        |p| {
                            p.inner.as_branch_mut().unwrap().chd[p_idx as usize] = Some(leaf);
                            p.root_hash = p.inner.hash(&self.store)
                        },
                        parents,
                        deleted
                    );
                }
                NodeType::Extension(n) => {
                    // from: P -> [p: Ext]x -> [b (v)]x -> [leaf]x
                    // to: P -> [Leaf (v)]
                    let leaf = self
                        .new_node(Node::new(
                            NodeType::Leaf(LeafNode(PartialPath(n.0.clone().into_inner()), val)),
                            &self.store,
                        ))
                        .unwrap()
                        .as_ptr();
                    deleted.push(p_ptr);
                    self.set_parent(leaf, parents);
                }
                _ => unreachable!(),
            }
        } else {
            let (c_ptr, idx) = b_chd.unwrap();
            let mut c_ref = self.get_node(c_ptr).unwrap();
            match &c_ref.inner {
                NodeType::Branch(_) => {
                    match &p_ref.inner {
                        NodeType::Branch(_) => {
                            //                            ____[Branch]
                            //                           /
                            // from: [p: Branch] -> [b]x*
                            //                           \____[Leaf]x
                            // to: [p: Branch] -> [Ext] -> [Branch]
                            let ext = self
                                .new_node(Node::new(
                                    NodeType::Extension(ExtNode(PartialPath(vec![idx]), c_ptr)),
                                    &self.store,
                                ))?
                                .as_ptr();
                            self.set_parent(ext, &mut [(p_ref, p_idx)]);
                        }
                        NodeType::Extension(_) => {
                            //                         ____[Branch]
                            //                        /
                            // from: [p: Ext] -> [b]x*
                            //                        \____[Leaf]x
                            // to: [p: Ext] -> [Branch]
                            write_node!(
                                self,
                                p_ref,
                                |p| {
                                    let mut pp = p.inner.as_extension_mut().unwrap();
                                    pp.0 .0.push(idx);
                                    pp.1 = c_ptr;
                                    p.root_hash = p.inner.hash(&self.store);
                                },
                                parents,
                                deleted
                            );
                        }
                        _ => unreachable!(),
                    }
                }
                NodeType::Leaf(_) | NodeType::Extension(_) => {
                    match &p_ref.inner {
                        NodeType::Branch(_) => {
                            //                            ____[Leaf/Ext]
                            //                           /
                            // from: [p: Branch] -> [b]x*
                            //                           \____[Leaf]x
                            // to: [p: Branch] -> [Leaf/Ext]
                            let c_ptr = if let None = c_ref.write(|c| {
                                (match &mut c.inner {
                                    NodeType::Leaf(n) => &mut n.0,
                                    NodeType::Extension(n) => &mut n.0,
                                    _ => unreachable!(),
                                })
                                .0
                                .insert(0, idx);
                                c.root_hash = c.inner.hash(&self.store)
                            }) {
                                deleted.push(c_ptr);
                                self.new_node(c_ref.clone())?.as_ptr()
                            } else {
                                c_ptr
                            };
                            write_node!(
                                self,
                                p_ref,
                                |p| {
                                    p.inner.as_branch_mut().unwrap().chd[p_idx as usize] = Some(c_ptr);
                                    p.root_hash = p.inner.hash(&self.store)
                                },
                                parents,
                                deleted
                            );
                        }
                        NodeType::Extension(n) => {
                            //                               ____[Leaf/Ext]
                            //                              /
                            // from: P -> [p: Ext]x -> [b]x*
                            //                              \____[Leaf]x
                            // to: P -> [p: Leaf/Ext]
                            deleted.push(p_ptr);
                            if !write_node!(
                                self,
                                c_ref,
                                |c| {
                                    let mut path = n.0.clone().into_inner();
                                    path.push(idx);
                                    let path0 = match &mut c.inner {
                                        NodeType::Leaf(n) => &mut n.0,
                                        NodeType::Extension(n) => &mut n.0,
                                        _ => unreachable!(),
                                    };
                                    path.extend(&**path0);
                                    *path0 = PartialPath(path);
                                    c.root_hash = c.inner.hash(&self.store)
                                },
                                parents,
                                deleted
                            ) {
                                self.set_parent(c_ptr, parents);
                            }
                        }
                        _ => unreachable!(),
                    }
                }
            }
        }
        Ok(())
    }

    fn after_remove_branch<'b>(
        &self, (c_ptr, idx): (ObjPtr<Node>, u8), parents: &mut Vec<(ObjRef<'b, Node>, u8)>,
        deleted: &mut Vec<ObjPtr<Node>>,
    ) -> Result<(), MerkleError> {
        // [b] -> [u] -> [c]
        let (mut b_ref, b_idx) = parents.pop().unwrap();
        let mut c_ref = self.get_node(c_ptr).unwrap();
        match &c_ref.inner {
            NodeType::Branch(_) => {
                write_node!(
                    self,
                    b_ref,
                    |b| {
                        match &mut b.inner {
                            NodeType::Branch(n) => {
                                // from: [Branch] -> [Branch]x -> [Branch]
                                // to: [Branch] -> [Ext] -> [Branch]
                                n.chd[b_idx as usize] = Some(
                                    self.new_node(Node::new(
                                        NodeType::Extension(ExtNode(PartialPath(vec![idx]), c_ptr)),
                                        &self.store,
                                    ))
                                    .unwrap()
                                    .as_ptr(),
                                );
                            }
                            NodeType::Extension(n) => {
                                // from: [Ext] -> [Branch]x -> [Branch]
                                // to: [Ext] -> [Branch]
                                n.0 .0.push(idx);
                                n.1 = c_ptr
                            }
                            _ => unreachable!(),
                        }
                        b.root_hash = b.inner.hash(&self.store)
                    },
                    parents,
                    deleted
                );
            }
            NodeType::Leaf(_) | NodeType::Extension(_) => match &b_ref.inner {
                NodeType::Branch(_) => {
                    // from: [Branch] -> [Branch]x -> [Leaf/Ext]
                    // to: [Branch] -> [Leaf/Ext]
                    let c_ptr = if let None = c_ref.write(|c| {
                        match &mut c.inner {
                            NodeType::Leaf(n) => &mut n.0,
                            NodeType::Extension(n) => &mut n.0,
                            _ => unreachable!(),
                        }
                        .0
                        .insert(0, idx);
                        c.root_hash = c.inner.hash(&self.store)
                    }) {
                        deleted.push(c_ptr);
                        self.new_node(c_ref.clone())?.as_ptr()
                    } else {
                        c_ptr
                    };
                    write_node!(
                        self,
                        b_ref,
                        |b| b.inner.as_branch_mut().unwrap().chd[b_idx as usize] = Some(c_ptr),
                        parents,
                        deleted
                    );
                }
                NodeType::Extension(n) => {
                    // from: P -> [Ext] -> [Branch]x -> [Leaf/Ext]
                    // to: P -> [Leaf/Ext]
                    let c_ptr = if let None = c_ref.write(|c| {
                        let mut path = n.0.clone().into_inner();
                        path.push(idx);
                        let path0 = match &mut c.inner {
                            NodeType::Leaf(n) => &mut n.0,
                            NodeType::Extension(n) => &mut n.0,
                            _ => unreachable!(),
                        };
                        path.extend(&**path0);
                        *path0 = PartialPath(path);
                        c.root_hash = c.inner.hash(&self.store)
                    }) {
                        deleted.push(c_ptr);
                        self.new_node(c_ref.clone())?.as_ptr()
                    } else {
                        c_ptr
                    };
                    self.set_parent(c_ptr, parents);
                }
                _ => unreachable!(),
            },
        }
        Ok(())
    }

    pub fn remove<K: AsRef<[u8]>>(&mut self, key: K) -> Result<bool, MerkleError> {
        let root = self.header.borrow().root;
        let mut chunks = vec![0];
        chunks.extend(to_nibbles(key.as_ref()));

        if root.is_null() {
            return Ok(false)
        }

        let mut deleted = Vec::new();
        let mut updated = Vec::new();
        let mut parents: Vec<(ObjRef<Node>, _)> = Vec::new();
        let mut u_ref = self.get_node(root)?;
        let mut nskip = 0;
        let mut found = false;

        for (i, nib) in chunks.iter().enumerate() {
            if nskip > 0 {
                nskip -= 1;
                continue
            }
            let next_ptr = match &u_ref.inner {
                NodeType::Branch(n) => match n.chd[*nib as usize] {
                    Some(c) => c,
                    None => return Ok(false),
                },
                NodeType::Leaf(n) => {
                    if &chunks[i..] != &*n.0 {
                        return Ok(false)
                    }
                    found = true;
                    deleted.push(u_ref.as_ptr());
                    self.after_remove_leaf(&mut parents, &mut deleted)?;
                    break
                }
                NodeType::Extension(n) => {
                    let n_path = &*n.0;
                    let rem_path = &chunks[i..];
                    if rem_path < n_path || &rem_path[..n_path.len()] != n_path {
                        return Ok(false)
                    }
                    nskip = n_path.len() - 1;
                    n.1
                }
            };

            updated.push(u_ref.as_ptr());
            parents.push((u_ref, *nib));
            u_ref = self.get_node(next_ptr)?;
        }
        if !found {
            match &u_ref.inner {
                NodeType::Branch(n) => {
                    if n.value.is_none() {
                        return Ok(false)
                    }
                    let (c_chd, _) = n.single_child();
                    u_ref
                        .write(|u| {
                            u.inner.as_branch_mut().unwrap().value = None;
                            u.root_hash = u.inner.hash(&self.store)
                        })
                        .unwrap();
                    found = true;
                    if let Some((c_ptr, idx)) = c_chd {
                        deleted.push(u_ref.as_ptr());
                        self.after_remove_branch((c_ptr, idx), &mut parents, &mut deleted)?
                    }
                }
                NodeType::Leaf(n) => {
                    if n.0.len() > 0 {
                        return Ok(false)
                    }
                    found = true;
                    deleted.push(u_ref.as_ptr());
                    self.after_remove_leaf(&mut parents, &mut deleted)?
                }
                _ => (),
            }
        }

        for ptr in updated.into_iter() {
            self.get_node(ptr)
                .unwrap()
                .write(|u| u.root_hash = u.inner.hash(&self.store))
                .unwrap();
        }

        drop(u_ref);
        drop(parents);
        for ptr in deleted.into_iter() {
            self.store.free_item(ptr).map_err(MerkleError::Shale)?;
        }
        Ok(found)
    }
}

fn to_nibbles<'a>(bytes: &'a [u8]) -> impl Iterator<Item = u8> + 'a {
    bytes.iter().flat_map(|b| [(b >> 4) & 0xf, b & 0xf].into_iter())
}

fn from_nibbles<'a>(nibbles: &'a [u8]) -> impl Iterator<Item = u8> + 'a {
    assert!(nibbles.len() & 1 == 0);
    nibbles.chunks_exact(2).map(|p| (p[0] << 4) | p[1])
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
