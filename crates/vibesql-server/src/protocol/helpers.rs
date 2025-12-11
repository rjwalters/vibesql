//! Protocol helper functions
//!
//! Utility functions for encoding and decoding protocol messages.

use std::collections::HashMap;

use bytes::{Buf, BufMut, BytesMut};

use super::types::ProtocolError;

/// Write a null-terminated C string
pub fn put_cstring(buf: &mut BytesMut, s: &str) {
    buf.put_slice(s.as_bytes());
    buf.put_u8(0);
}

/// Read a null-terminated C string
pub fn read_cstring(buf: &mut BytesMut) -> Result<String, ProtocolError> {
    let null_pos = buf.iter().position(|&b| b == 0).ok_or(ProtocolError::InvalidString)?;

    let bytes = buf.split_to(null_pos);
    buf.advance(1); // skip null byte

    String::from_utf8(bytes.to_vec()).map_err(|_| ProtocolError::InvalidString)
}

/// Encode error or notice response fields
pub fn encode_notice_or_error(buf: &mut BytesMut, fields: &HashMap<u8, String>) {
    // Calculate length
    let mut len = 4 + 1; // length field + terminator
    for value in fields.values() {
        len += 1 + value.len() + 1; // field type + value + null
    }

    buf.put_i32(len as i32);

    // Write fields
    for (&field_type, value) in fields {
        buf.put_u8(field_type);
        put_cstring(buf, value);
    }

    // Terminator
    buf.put_u8(0);
}
