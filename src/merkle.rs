use parking_lot::{Mutex, MutexGuard};
use shale::{LinearMemImage, MummyItem, ObjPtr, ObjRef, ShaleError, ShaleStore, WriteContext};
use std::sync::Arc;

const NBRANCH: usize = 16;

pub struct Hash([u8; 32]);

pub enum MerkleError {
    Shale(shale::ShaleError),
}

pub struct MerkleHeader {
    root: ObjPtr<Node>,
}

// the partial path is prefixed by a flag
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

struct Data(Vec<u8>);

struct BranchNode {
    chd: [Option<ObjPtr<Node>>; NBRANCH],
    value: Option<Data>,
}

struct LeafNode(EncodedPath, ObjPtr<Data>);

struct ExtNode(EncodedPath, ObjPtr<Node>);

enum Node {
    Branch(Hash, BranchNode),
    Leaf(Hash, LeafNode),
    Extension(Hash, ExtNode),
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
                Self::Branch(
                    hash,
                    BranchNode {
                        chd,
                        value: if items[NBRANCH].len() > 0 {
                            Some(Data(items[NBRANCH].to_vec()))
                        } else {
                            None
                        },
                    },
                ),
            ))
        } else if items.len() == 2 {
            let (path, term) = EncodedPath::decode(&items[0]);
            Ok((
                obj_len,
                if term {
                    Self::Leaf(
                        hash,
                        LeafNode(path, unsafe {
                            ObjPtr::new_from_addr(u64::from_le_bytes(
                                items[1][..8].try_into().map_err(dec_err)?,
                            ))
                        }),
                    )
                } else {
                    Self::Extension(
                        hash,
                        ExtNode(path, unsafe {
                            ObjPtr::new_from_addr(u64::from_le_bytes(
                                items[1][..8].try_into().map_err(dec_err)?,
                            ))
                        }),
                    )
                },
            ))
        } else {
            Err(ShaleError::DecodeError)
        }
    }

    fn dehydrate(&self) -> Vec<u8> {
        let mut items;
        let hash;
        match self {
            Self::Branch(h, n) => {
                items = Vec::new();
                hash = h;
                for c in n.chd.iter() {
                    items.push(match c {
                        Some(p) => p.addr().to_le_bytes().to_vec(),
                        None => Vec::new(),
                    });
                }
            }
            Self::Extension(h, n) => {
                items = vec![n.0.encode(false), n.1.addr().to_le_bytes().to_vec()];
                hash = h
            }
            Self::Leaf(h, n) => {
                items = vec![n.0.encode(true), n.1.addr().to_le_bytes().to_vec()];
                hash = h
            }
        }
        let mut m = Vec::new();
        let rlp_encoded = rlp::encode_list::<Vec<u8>, _>(&items);
        m.extend(hash.0);
        m.extend((rlp_encoded.len() as u64).to_le_bytes().to_vec());
        m.extend(rlp_encoded);
        m
    }
}

pub struct MerkleInner<S: ShaleStore> {
    header: ObjRef<'static, MerkleHeader>,
    store: S,
}

pub struct Merkle<S: ShaleStore>(Mutex<MerkleInner<S>>);

pub struct MerkleBatch<'a, S: ShaleStore> {
    m: MutexGuard<'a, MerkleInner<S>>,
    wctx: WriteContext,
}

impl<S: ShaleStore> Merkle<S> {
    pub fn new(header: ObjRef<'static, MerkleHeader>, store: S) -> Self {
        Self(Mutex::new(MerkleInner { header, store }))
    }

    pub fn new_batch(&self) -> MerkleBatch<S> {
        MerkleBatch {
            m: self.0.lock(),
            wctx: WriteContext::new(),
        }
    }
}

impl<'a, S: ShaleStore> MerkleBatch<'a, S> {
    pub fn insert(&mut self, key: &[u8], val: Vec<u8>) -> Result<(), MerkleError> {
        let u = self
            .m
            .store
            .get_item(self.m.header.root)
            .map_err(MerkleError::Shale)?;
        Ok(())
    }
}
