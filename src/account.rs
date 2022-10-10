use crate::merkle::Node;
use primitive_types::U256;
use shale::{MemStore, MummyItem, ObjPtr, ShaleError};

pub struct Code(Vec<u8>);

impl MummyItem for Code {
    fn hydrate(addr: u64, mem: &dyn MemStore) -> Result<(u64, Self), ShaleError> {
        let raw = mem.get_view(addr, 4).ok_or(ShaleError::LinearMemStoreError)?;
        let len = u32::from_le_bytes(raw[..].try_into().unwrap()) as u64;
        let bytes = mem.get_view(addr + 4, len).ok_or(ShaleError::LinearMemStoreError)?;
        Ok((4 + len, Self(bytes.to_vec())))
    }

    fn dehydrate(&self) -> Vec<u8> {
        let mut buff = Vec::new();
        buff.extend((self.0.len() as u32).to_le_bytes());
        buff.extend(&self.0);
        buff
    }
}

pub struct Account {
    nonce: u64,
    balance: U256,
    root: ObjPtr<Node>,
    code: ObjPtr<Code>,
}

impl Account {
    fn serialize(&self) -> Vec<u8> {
        let mut buff = Vec::new();
        buff.extend(self.nonce.to_le_bytes());
        buff.resize(40, 0);
        self.balance.to_big_endian(&mut buff[8..40]);
        buff.extend((self.root.addr() as u64).to_le_bytes());
        buff.extend((self.code.addr() as u64).to_le_bytes());
        buff
    }

    fn deserialize(raw: &[u8]) -> Self {
        let nonce = u64::from_le_bytes(raw[..8].try_into().unwrap());
        let balance = U256::from_big_endian(&raw[8..40]);
        let root = u64::from_le_bytes(raw[40..48].try_into().unwrap());
        let code = u64::from_le_bytes(raw[48..].try_into().unwrap());
        unsafe {
            Self {
                nonce,
                balance,
                root: ObjPtr::new_from_addr(root),
                code: ObjPtr::new_from_addr(code),
            }
        }
    }
}
