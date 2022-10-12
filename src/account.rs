use crate::merkle::{Hash, Node, ValueTransformer};
use primitive_types::U256;
use shale::{MemStore, MummyItem, ObjPtr, ObjRef, ShaleError, ShaleStore};

pub struct Account {
    pub nonce: u64,
    pub balance: U256,
    pub root: ObjPtr<Node>,
    pub code: ObjPtr<Blob>,
    pub root_hash: Hash,
    pub code_hash: Hash,
}

impl Account {
    pub fn empty_code() -> &'static Hash {
        use once_cell::sync::OnceCell;
        static V: OnceCell<Hash> = OnceCell::new();
        V.get_or_init(|| {
            Hash(
                hex::decode("c5d2460186f7233c927e7db2dcc703c0e500b653ca82273b7bfad8045d85a470")
                    .unwrap()
                    .try_into()
                    .unwrap(),
            )
        })
    }

    pub fn serialize(&self) -> Vec<u8> {
        let mut buff = Vec::new();
        buff.extend(self.nonce.to_le_bytes());
        buff.resize(40, 0);
        self.balance.to_big_endian(&mut buff[8..40]);
        buff.extend((self.root.addr() as u64).to_le_bytes());
        buff.extend((self.code.addr() as u64).to_le_bytes());
        buff.extend(self.root_hash.0);
        buff.extend(self.code_hash.0);
        buff
    }

    pub fn deserialize(raw: &[u8]) -> Self {
        let nonce = u64::from_le_bytes(raw[..8].try_into().unwrap());
        let balance = U256::from_big_endian(&raw[8..40]);
        let root = u64::from_le_bytes(raw[40..48].try_into().unwrap());
        let code = u64::from_le_bytes(raw[48..56].try_into().unwrap());
        let root_hash = Hash(raw[56..88].try_into().unwrap());
        let code_hash = Hash(raw[88..].try_into().unwrap());

        unsafe {
            Self {
                nonce,
                balance,
                root: ObjPtr::new_from_addr(root),
                code: ObjPtr::new_from_addr(code),
                root_hash,
                code_hash,
            }
        }
    }

    pub fn set_code(&mut self, code_hash: Hash, code: ObjPtr<Blob>) {
        self.code_hash = code_hash;
        self.code = code;
    }
}

pub struct AccountRLP;

impl ValueTransformer for AccountRLP {
    fn transform(raw: &[u8]) -> Vec<u8> {
        let acc = Account::deserialize(raw);
        let mut stream = rlp::RlpStream::new_list(4);
        stream.append(&acc.nonce);
        stream.append(&acc.balance);
        stream.append(&&acc.root_hash[..]);
        stream.append(&&acc.code_hash[..]);
        stream.out().into()
    }
}

impl Default for Account {
    fn default() -> Self {
        Account {
            nonce: 0,
            balance: U256::zero(),
            root: ObjPtr::null(),
            code: ObjPtr::null(),
            root_hash: crate::merkle::Merkle::empty_root().clone(),
            code_hash: Self::empty_code().clone(),
        }
    }
}

pub enum Blob {
    Code(Vec<u8>),
}

impl MummyItem for Blob {
    // currently there is only one variant of Blob: Code
    fn hydrate(addr: u64, mem: &dyn MemStore) -> Result<(u64, Self), ShaleError> {
        let raw = mem.get_view(addr, 4).ok_or(ShaleError::LinearMemStoreError)?;
        let len = u32::from_le_bytes(raw[..].try_into().unwrap()) as u64;
        let bytes = mem.get_view(addr + 4, len).ok_or(ShaleError::LinearMemStoreError)?;
        Ok((4 + len, Self::Code(bytes.to_vec())))
    }

    fn dehydrate(&self) -> Vec<u8> {
        match self {
            Self::Code(code) => {
                let mut buff = Vec::new();
                buff.extend((code.len() as u32).to_le_bytes());
                buff.extend(code);
                buff
            }
        }
    }
}

#[derive(Debug)]
pub enum BlobError {
    Shale(ShaleError),
}

pub struct BlobStash {
    store: Box<dyn ShaleStore<Blob>>,
}

impl BlobStash {
    pub fn new(store: Box<dyn ShaleStore<Blob>>) -> Self {
        Self { store }
    }

    pub fn get_blob(&self, ptr: ObjPtr<Blob>) -> Result<ObjRef<Blob>, BlobError> {
        self.store.get_item(ptr).map_err(BlobError::Shale)
    }

    pub fn new_blob(&self, item: Blob) -> Result<ObjRef<Blob>, BlobError> {
        self.store.put_item(item, 0).map_err(BlobError::Shale)
    }

    pub fn free_blob(&mut self, ptr: ObjPtr<Blob>) -> Result<(), BlobError> {
        self.store.free_item(ptr).map_err(BlobError::Shale)
    }
}
