use enum_as_inner::EnumAsInner;
use parking_lot::{Mutex, MutexGuard};
use shale::{LinearMemImage, MummyItem, ObjPtr, ObjRef, ShaleError, ShaleStore, WriteContext};
use std::cell::RefCell;

const NBRANCH: usize = 16;

pub struct Hash([u8; 32]);

impl MummyItem for Hash {
    fn hydrate(addr: u64, mem: &dyn LinearMemImage) -> Result<(u64, Self), ShaleError> {
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

pub enum MerkleError {
    Shale(shale::ShaleError),
}

pub struct MerkleHeader {
    root: ObjPtr<Node>,
}

// the partial path is prefixed by a flag, the decoded form is an array of nibbles
struct EncodedPath(Vec<u8>);

impl EncodedPath {
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

#[derive(Clone)]
struct Data(Vec<u8>);

struct BranchNode {
    chd: [Option<ObjPtr<Node>>; NBRANCH],
    value: Option<Data>,
}

struct LeafNode(EncodedPath, Data);

struct ExtNode(EncodedPath, ObjPtr<Node>);

struct Node {
    root_hash: Hash,
    inner: NodeType,
}

#[derive(EnumAsInner)]
enum NodeType {
    Branch(BranchNode),
    Leaf(LeafNode),
    Extension(ExtNode),
}

impl MummyItem for Node {
    fn hydrate(addr: u64, mem: &dyn LinearMemImage) -> Result<(u64, Self), ShaleError> {
        let dec_err = |_| ShaleError::DecodeError;
        // this is different from geth due to the different addressing system, so pointer (8-byte)
        // values are used in place of hashes to refer to a child node.
        let meta_raw = mem
            .get_ref(addr, 8 + 32)
            .ok_or(ShaleError::LinearMemStoreError)?;
        let hash = Hash(meta_raw[0..32].try_into().map_err(dec_err)?);
        let len = u64::from_le_bytes(meta_raw[32..].try_into().map_err(dec_err)?);
        let obj_len = 8 + 32 + len;
        let rlp_raw = mem
            .get_ref(addr, len)
            .ok_or(ShaleError::LinearMemStoreError)?;
        let items: Vec<Vec<u8>> = rlp::decode_list(&rlp_raw);
        if items.len() == 0 {
            return Err(ShaleError::DecodeError);
        }
        let items = &items[1..];
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
            let (path, term) = EncodedPath::decode(&items[0]);
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
            }
            NodeType::Extension(n) => {
                items = vec![n.0.encode(false), n.1.addr().to_le_bytes().to_vec()];
            }
            NodeType::Leaf(n) => {
                items = vec![n.0.encode(true), n.1 .0.clone()];
            }
        }
        let mut m = Vec::new();
        let rlp_encoded = rlp::encode_list::<Vec<u8>, _>(&items);
        m.extend(self.root_hash.0);
        m.extend((rlp_encoded.len() as u64).to_le_bytes().to_vec());
        m.extend(rlp_encoded);
        m
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
                // now we need to split
                u_ref.write(
                    |u| {
                        *(match &mut u.inner {
                            NodeType::Leaf(u) => &mut u.0,
                            NodeType::Extension(u) => &mut u.0,
                            _ => unreachable!(),
                        }) = EncodedPath(n_path[idx + 1..].to_vec())
                    },
                    true,
                    &self.wctx,
                );
                let leaf = Node {
                    root_hash: Hash([0; 32]),
                    inner: NodeType::Leaf(LeafNode(
                        EncodedPath(rem_path[idx + 1..].to_vec()),
                        Data(val),
                    )),
                };
                let leaf_r = self.new_node(leaf)?;
                let mut chd = [None; NBRANCH];
                chd[rem_path[idx] as usize] = Some(leaf_r.as_ptr());
                chd[n_path[idx] as usize] = Some(u_ptr);
                let branch = Node {
                    root_hash: Hash([0; 32]),
                    inner: NodeType::Branch(BranchNode { chd, value: None }),
                };
                let branch_r = self.new_node(branch)?;
                if idx > 0 {
                    let ext = Node {
                        root_hash: Hash([0; 32]),
                        inner: NodeType::Extension(ExtNode(
                            EncodedPath(rem_path[..idx].to_vec()),
                            branch_r.as_ptr(),
                        )),
                    };
                    self.new_node(ext)?
                } else {
                    branch_r
                }
            }
            None => {
                let (leaf_ptr, prefix, idx, v) = if rem_path.len() < n_path.len() {
                    u_ref.write(
                        |u| {
                            *(match &mut u.inner {
                                NodeType::Leaf(u) => &mut u.0,
                                NodeType::Extension(u) => &mut u.0,
                                _ => unreachable!(),
                            }) = EncodedPath(n_path[rem_path.len() + 1..].to_vec())
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
                    if n_value.is_none() {
                        // this case does not apply to an extension node, resume the tree walk
                        return Ok(Some(val));
                    }
                    let leaf = Node {
                        root_hash: Hash([0; 32]),
                        inner: NodeType::Leaf(LeafNode(
                            EncodedPath(rem_path[n_path.len() + 1..].to_vec()),
                            Data(val),
                        )),
                    };
                    let leaf_r = self.new_node(leaf)?;
                    deleted.push(u_ptr);
                    (
                        leaf_r.as_ptr(),
                        &n_path[..],
                        rem_path[n_path.len()],
                        n_value,
                    )
                };
                let mut chd = [None; NBRANCH];
                chd[idx as usize] = Some(leaf_ptr);
                let branch = Node {
                    root_hash: Hash([0; 32]),
                    inner: NodeType::Branch(BranchNode { chd, value: v }),
                };
                let branch_r = self.new_node(branch)?;
                let ext = Node {
                    root_hash: Hash([0; 32]),
                    inner: NodeType::Extension(ExtNode(
                        EncodedPath(prefix.to_vec()),
                        branch_r.as_ptr(),
                    )),
                };
                self.new_node(ext)?
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

    fn insert_by_case<'b>(
        &self,
        parent: Option<&mut (ObjRef<'b, Node>, u8)>,
        u_ref: &mut ObjRef<'b, Node>,
        i: usize,
        nib: u8,
        chunks: &[u8],
        val: Vec<u8>,
        deleted: &mut Vec<ObjPtr<Node>>,
        new_root: &mut Option<ObjPtr<Node>>,
    ) -> Result<Option<(ObjPtr<Node>, Vec<u8>)>, MerkleError> {
        match &u_ref.inner {
            NodeType::Branch(n) => match n.chd[nib as usize] {
                Some(c) => return Ok(Some((c, val))),
                None => {
                    // create a leaf node
                    let leaf = Node {
                        root_hash: Hash([0; 32]),
                        inner: NodeType::Leaf(LeafNode(
                            EncodedPath(chunks[i + 1..].to_vec()),
                            Data(val),
                        )),
                    };
                    let leaf_r = self.new_node(leaf)?;
                    u_ref.write(
                        |u| {
                            u.inner.as_branch_mut().unwrap().chd[nib as usize] =
                                Some(leaf_r.as_ptr());
                        },
                        true,
                        &self.wctx,
                    );
                    return Ok(None);
                }
            },
            NodeType::Leaf(n) => {
                let n_path = n.0 .0.to_vec();
                let n_value = Some(n.1.clone());
                self.split(
                    u_ref,
                    parent,
                    &chunks[i + 1..],
                    n_path,
                    n_value,
                    val,
                    deleted,
                    new_root,
                )?;
                return Ok(None);
            }
            NodeType::Extension(n) => {
                let n_path = n.0 .0.to_vec();
                let n_ptr = n.1;
                if let Some(val) = self.split(
                    u_ref,
                    parent,
                    &chunks[i + 1..],
                    n_path,
                    None,
                    val,
                    deleted,
                    new_root,
                )? {
                    return Ok(Some((n_ptr, val)));
                }
                return Ok(None);
            }
        }
    }
    pub fn insert(&mut self, key: &[u8], mut val: Vec<u8>) -> Result<(), MerkleError> {
        let mut deleted = Vec::new();
        {
            let mut u_ref = self.get_node(self.m.header.borrow().root)?;
            let chunks: Vec<_> = to_nibbles(key).collect();
            let mut parent = None;
            for (i, nib) in chunks.iter().enumerate() {
                let mut new_root = None;
                match self.insert_by_case(
                    parent.as_mut(),
                    &mut u_ref,
                    i,
                    *nib,
                    &chunks,
                    val,
                    &mut deleted,
                    &mut new_root,
                )? {
                    Some((next_ptr, v)) => {
                        parent = Some((u_ref, *nib));
                        u_ref = self.get_node(next_ptr)?;
                        val = v
                    }
                    None => {
                        if let Some(nr) = new_root {
                            self.m
                                .header
                                .borrow_mut()
                                .write(|r| r.root = nr, true, &self.wctx);
                        }
                        break;
                    }
                }
            }
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
