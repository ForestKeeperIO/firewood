pub mod utils;

#[cfg(test)]
mod tests {
    use shale::compact::{CompactHeader, CompactSpace, CompactSpaceHeader};
    use shale::{to_dehydrated, DynamicMem, MummyObj, ObjCache, ObjPtr};

    use shale::{MemStore, MummyItem, ShaleStore};
    use std::rc::Rc;

    use crate::utils::*;

    #[test]
    fn test_compact_space() {
        let meta_size = 0x10000;
        let compact_size = 0x10000;
        const RESERVED: u64 = 0x1000;

        let (db_fd, reset) = open_dir("/tmp/stuff", false).unwrap();

        let merkle_fd = touch_dir("merkle", db_fd).unwrap();
        let merkle_meta_fd = touch_dir("meta", merkle_fd).unwrap();
        let merkle_payload_fd = touch_dir("compact", merkle_fd).unwrap();

        let mut dm = DynamicMem::new(meta_size, 0);

        let compact_header: ObjPtr<CompactSpaceHeader> = ObjPtr::new_from_addr(0x0);

        dm.write(
            compact_header.addr(),
            &to_dehydrated(&CompactSpaceHeader::new(RESERVED, RESERVED)),
        );
        let compact_header =
            MummyObj::ptr_to_obj(&dm, compact_header, CompactHeader::MSIZE).unwrap();
        let mem_meta = Rc::new(dm);
        let mem_payload = Rc::new(DynamicMem::new(compact_size, 0x1));

        let mut space: CompactSpace<Hash, DynamicMem> = CompactSpace::new(
            mem_meta,
            mem_payload,
            compact_header,
            ObjCache::new(1),
            10,
            16,
        )
        .expect("CompactSpace init fail");

        let buf: &mut [u8] = &mut [];

        let resp = space.put_item(Hash([1; 32]), 1).unwrap();
        // print!("deref {:?}", resp.inner);
        print!("{:?}", resp.get_space_id());

        let resp = space.put_item(Hash([2; 32]), 1).unwrap();
        print!("{:?}", resp.0);
        print!("len {:?}", resp.dehydrated_len());

        // let resp = space.free_item(resp);
        // assert!(resp.is_ok());
    }
}
