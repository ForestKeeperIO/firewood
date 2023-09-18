#![no_main]

use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    // fuzzed code goes here
    if let Ok(decoded)  = firewood::codec::decode_int(&data) {
        let encoded = firewood::codec::encode_int(decoded.0).unwrap();
        let expected = &data[..decoded.1];
        assert_eq!(&encoded, expected);
    }
});
