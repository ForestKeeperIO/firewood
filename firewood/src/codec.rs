// Copyright (C) 2023, Ava Labs, Inc. All rights reserved.
// See the file LICENSE.md for licensing terms.

use bincode::Options;
use thiserror::Error;

const MIN_BYTES_LEN: usize = 1;

#[derive(Debug, Error)]
pub enum CodecError {
    #[error("bincode error")]
    BincodeError(#[from] bincode::Error),
    #[error("no such node")]
    UnexpectedEOFError,
    #[error("from utf8 error")]
    FromUtf8Error(#[from] std::string::FromUtf8Error),
    #[error("leading zeroes error")]
    LeadingZeroesError,
}

use serde::Deserialize;

#[derive(Deserialize, Debug)]
struct Data {
    val: i64,

    #[serde(skip)]
    trailing: Vec<u8>,
}

fn decode(src: &[u8]) -> Result<Data, CodecError> {
    let mut cursor = src;
    let mut data: Data = bincode::DefaultOptions::new().deserialize_from(&mut cursor)?;
    data.trailing = cursor.to_owned();
    Ok(data)
}

pub fn encode_int(val: i64) -> Result<Vec<u8>, CodecError> {
    Ok(bincode::DefaultOptions::new().serialize(&val)?)
}

pub fn decode_int(src: &[u8]) -> Result<(i64, usize), CodecError> {
    // To ensure encoding/decoding is canonical, we need to check for leading
	// zeroes in the varint.
	// The last byte of the varint we read is the most significant byte.
	// If it's 0, then it's a leading zero, which is considered invalid in the
	// canonical encoding.
    let data = decode(src)?;
    let len = src.len() - data.trailing.len();

    // Just 0x00 is a valid value so don't check if the varint is 1 byte
	if len > 1 && src[len-1] == 0x00 {
		return Err(CodecError::LeadingZeroesError)
	}
    Ok((data.val, len))
}

pub fn encode_str(val: &[u8]) -> Result<Vec<u8>, CodecError> {
    let res = encode_int(val.len() as i64)?;
    Ok([&res[..], val].concat())
}

pub fn decode_str(src: &[u8]) -> Result<String, CodecError> {
    if src.len() < MIN_BYTES_LEN {
        return Err(CodecError::UnexpectedEOFError);
    }

    let data = decode(src)?;
    let len = data.val;
    if len < 0 {
        return Err(CodecError::UnexpectedEOFError);
    } else if len == 0 {
        return Ok(String::default());
    } else if len as usize > src.len() {
        return Err(CodecError::UnexpectedEOFError);
    }
    Ok(String::from_utf8(data.trailing)?)
}
