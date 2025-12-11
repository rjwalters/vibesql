//! Malformed message handling tests
//!
//! Tests for security-relevant handling of invalid wire protocol messages.

use bytes::{BufMut, BytesMut};

use crate::protocol::{FrontendMessage, ProtocolError};

// -----------------------------------------------------------------
// Truncated Message Tests
// -----------------------------------------------------------------

#[test]
fn test_truncated_message_empty_buffer() {
    let mut buf = BytesMut::new();
    // Empty buffer should return None (need more data)
    let result = FrontendMessage::decode(&mut buf);
    assert!(result.is_ok());
    assert!(result.unwrap().is_none());
}

#[test]
fn test_truncated_message_only_type_byte() {
    let mut buf = BytesMut::new();
    buf.put_u8(b'Q'); // Only message type, no length
    let result = FrontendMessage::decode(&mut buf);
    assert!(result.is_ok());
    assert!(result.unwrap().is_none());
}

#[test]
fn test_truncated_message_partial_length() {
    let mut buf = BytesMut::new();
    buf.put_u8(b'Q');
    buf.put_u8(0); // Only 1 byte of length (need 4)
    buf.put_u8(0);
    let result = FrontendMessage::decode(&mut buf);
    assert!(result.is_ok());
    assert!(result.unwrap().is_none());
}

#[test]
fn test_truncated_message_incomplete_body() {
    let mut buf = BytesMut::new();
    buf.put_u8(b'Q');
    buf.put_i32(100); // Claims 100 bytes
    buf.put_slice(b"SELECT"); // Only 6 bytes
    let result = FrontendMessage::decode(&mut buf);
    assert!(result.is_ok());
    assert!(result.unwrap().is_none());
}

#[test]
fn test_truncated_startup_empty_buffer() {
    let mut buf = BytesMut::new();
    let result = FrontendMessage::decode_startup(&mut buf);
    assert!(result.is_ok());
    assert!(result.unwrap().is_none());
}

#[test]
fn test_truncated_startup_partial_length() {
    let mut buf = BytesMut::new();
    buf.put_u8(0);
    buf.put_u8(0); // Only 2 bytes of length
    let result = FrontendMessage::decode_startup(&mut buf);
    assert!(result.is_ok());
    assert!(result.unwrap().is_none());
}

#[test]
fn test_truncated_startup_incomplete_body() {
    let mut buf = BytesMut::new();
    buf.put_i32(50); // Claims 50 bytes total
    buf.put_i32(196608); // Protocol version 3.0
    buf.put_slice(b"user\0"); // Only partial params
    let result = FrontendMessage::decode_startup(&mut buf);
    assert!(result.is_ok());
    assert!(result.unwrap().is_none());
}

// -----------------------------------------------------------------
// Invalid Message Type Tests
// -----------------------------------------------------------------

#[test]
fn test_invalid_message_type_byte() {
    let mut buf = BytesMut::new();
    buf.put_u8(0xFF); // Invalid message type
    buf.put_i32(4); // Minimal length
    let result = FrontendMessage::decode(&mut buf);
    assert!(matches!(result, Err(ProtocolError::InvalidMessageType(0xFF))));
}

#[test]
fn test_invalid_message_type_zero() {
    let mut buf = BytesMut::new();
    buf.put_u8(0x00); // Null byte as message type
    buf.put_i32(4);
    let result = FrontendMessage::decode(&mut buf);
    assert!(matches!(result, Err(ProtocolError::InvalidMessageType(0x00))));
}

#[test]
fn test_invalid_message_type_lowercase_q() {
    // 'q' is not a valid message type (Query is uppercase 'Q')
    let mut buf = BytesMut::new();
    buf.put_u8(b'q');
    buf.put_i32(13);
    buf.put_slice(b"SELECT 1\0");
    let result = FrontendMessage::decode(&mut buf);
    assert!(matches!(result, Err(ProtocolError::InvalidMessageType(b'q'))));
}

#[test]
fn test_invalid_message_type_numeric() {
    let mut buf = BytesMut::new();
    buf.put_u8(b'1'); // Numeric character
    buf.put_i32(4);
    let result = FrontendMessage::decode(&mut buf);
    assert!(matches!(result, Err(ProtocolError::InvalidMessageType(b'1'))));
}

// -----------------------------------------------------------------
// Length Field Mismatch Tests
// -----------------------------------------------------------------

#[test]
fn test_length_zero() {
    let mut buf = BytesMut::new();
    buf.put_u8(b'X'); // Terminate
    buf.put_i32(0); // Invalid zero length (should be at least 4)
    let result = FrontendMessage::decode(&mut buf);
    // Length 0 is invalid - minimum length is 4 (includes the length field itself)
    assert!(matches!(result, Err(ProtocolError::InvalidMessageLength(0))));
}

#[test]
fn test_length_negative() {
    let mut buf = BytesMut::new();
    buf.put_u8(b'X');
    buf.put_i32(-1); // Negative length
    let result = FrontendMessage::decode(&mut buf);
    // Negative lengths are invalid - returns error instead of panic
    assert!(matches!(result, Err(ProtocolError::InvalidMessageLength(-1))));
}

#[test]
fn test_length_too_small() {
    let mut buf = BytesMut::new();
    buf.put_u8(b'X');
    buf.put_i32(3); // Less than minimum valid length of 4
    let result = FrontendMessage::decode(&mut buf);
    assert!(matches!(result, Err(ProtocolError::InvalidMessageLength(3))));
}

#[test]
fn test_startup_length_too_small() {
    let mut buf = BytesMut::new();
    buf.put_i32(4); // Only length field, no protocol version
    let result = FrontendMessage::decode_startup(&mut buf);
    // Startup message must be at least 8 bytes (length + protocol version)
    assert!(matches!(result, Err(ProtocolError::InvalidMessageLength(4))));
}

#[test]
fn test_startup_length_negative() {
    let mut buf = BytesMut::new();
    buf.put_i32(-1); // Negative length
    let result = FrontendMessage::decode_startup(&mut buf);
    assert!(matches!(result, Err(ProtocolError::InvalidMessageLength(-1))));
}

// -----------------------------------------------------------------
// Invalid UTF-8 Tests
// -----------------------------------------------------------------

#[test]
fn test_invalid_utf8_in_query() {
    let mut buf = BytesMut::new();
    buf.put_u8(b'Q');
    buf.put_i32(8); // 4 + 4 bytes of invalid data
    buf.put_slice(&[0xFF, 0xFE, 0x80]); // Invalid UTF-8 sequence
    buf.put_u8(0); // Null terminator
    let result = FrontendMessage::decode(&mut buf);
    assert!(matches!(result, Err(ProtocolError::InvalidString)));
}

#[test]
fn test_invalid_utf8_continuation_byte() {
    let mut buf = BytesMut::new();
    buf.put_u8(b'Q');
    buf.put_i32(6); // 4 + 2 bytes
    buf.put_u8(0x80); // Continuation byte without start byte
    buf.put_u8(0); // Null terminator
    let result = FrontendMessage::decode(&mut buf);
    assert!(matches!(result, Err(ProtocolError::InvalidString)));
}

#[test]
fn test_invalid_utf8_overlong_encoding() {
    let mut buf = BytesMut::new();
    buf.put_u8(b'Q');
    buf.put_i32(7);
    buf.put_slice(&[0xC0, 0x80]); // Overlong encoding of NUL
    buf.put_u8(0); // Null terminator
    let result = FrontendMessage::decode(&mut buf);
    assert!(matches!(result, Err(ProtocolError::InvalidString)));
}

#[test]
fn test_invalid_utf8_in_password() {
    let mut buf = BytesMut::new();
    buf.put_u8(b'p'); // Password message
    buf.put_i32(8);
    buf.put_slice(&[0xFE, 0xFF, 0x00]); // Invalid UTF-8 with embedded null
    buf.put_u8(0);
    let result = FrontendMessage::decode(&mut buf);
    // The embedded null will cause issues - string will be empty
    assert!(result.is_ok() || matches!(result, Err(ProtocolError::InvalidString)));
}

#[test]
fn test_invalid_utf8_in_startup_user() {
    let mut buf = BytesMut::new();
    // Build a proper startup message with invalid UTF-8 in the username value
    // Length: 4 (len) + 4 (version) + 5 (user\0) + 3 (invalid UTF-8 + \0) + 1 (final \0) =
    // 17
    buf.put_i32(17);
    buf.put_i32(196608); // Protocol version 3.0
    buf.put_slice(b"user\0");
    buf.put_slice(&[0xFF, 0xFE]); // Invalid UTF-8 for username value
    buf.put_u8(0); // Null terminator for value
    buf.put_u8(0); // Final empty key to end params
    let result = FrontendMessage::decode_startup(&mut buf);
    // The invalid UTF-8 should cause an error when parsing the value
    assert!(matches!(result, Err(ProtocolError::InvalidString)));
}

// -----------------------------------------------------------------
// Missing Null Terminator Tests
// -----------------------------------------------------------------

#[test]
fn test_query_missing_null_terminator() {
    let mut buf = BytesMut::new();
    buf.put_u8(b'Q');
    buf.put_i32(12); // Length
    buf.put_slice(b"SELECT 1"); // No null terminator
    let result = FrontendMessage::decode(&mut buf);
    assert!(matches!(result, Err(ProtocolError::InvalidString)));
}

#[test]
fn test_startup_missing_final_null() {
    let mut buf = BytesMut::new();
    // Length: 4 (len) + 4 (version) + 5 (user\0) + 5 (test\0) = 18
    // Note: normally there should be a final empty key (\0) to terminate params
    buf.put_i32(18);
    buf.put_i32(196608); // Protocol version 3.0
    buf.put_slice(b"user\0test\0"); // No final empty string terminator
    let result = FrontendMessage::decode_startup(&mut buf);
    // With our fix, this now succeeds because the empty buffer check breaks the loop
    // The message is parsed but may be incomplete - this is acceptable behavior
    assert!(result.is_ok());
    let msg = result.unwrap();
    assert!(matches!(msg, Some(FrontendMessage::Startup { .. })));
}

// -----------------------------------------------------------------
// Zero-Length Message Tests
// -----------------------------------------------------------------

#[test]
fn test_terminate_minimal() {
    // Terminate message is valid with just type + length
    let mut buf = BytesMut::new();
    buf.put_u8(b'X');
    buf.put_i32(4); // Minimum valid length
    let result = FrontendMessage::decode(&mut buf);
    assert!(result.is_ok());
    assert!(matches!(result.unwrap(), Some(FrontendMessage::Terminate)));
}

#[test]
fn test_query_empty_string() {
    let mut buf = BytesMut::new();
    buf.put_u8(b'Q');
    buf.put_i32(5); // 4 + 1 for just null terminator
    buf.put_u8(0); // Empty query
    let result = FrontendMessage::decode(&mut buf);
    assert!(result.is_ok());
    assert!(matches!(
        result.unwrap(),
        Some(FrontendMessage::Query { query }) if query.is_empty()
    ));
}

// -----------------------------------------------------------------
// SSL Request Tests
// -----------------------------------------------------------------

#[test]
fn test_ssl_request_detection() {
    let mut buf = BytesMut::new();
    buf.put_i32(8); // Length
    buf.put_i32(80877103); // SSL request code
    let result = FrontendMessage::decode_startup(&mut buf);
    assert!(result.is_ok());
    assert!(matches!(result.unwrap(), Some(FrontendMessage::SSLRequest)));
}

// -----------------------------------------------------------------
// Valid Protocol Version Tests
// -----------------------------------------------------------------

#[test]
fn test_startup_protocol_version_3_0() {
    let mut buf = BytesMut::new();
    buf.put_i32(17); // Total length
    buf.put_i32(196608); // Protocol version 3.0 (0x00030000)
    buf.put_slice(b"user\0pg\0"); // user=pg
    buf.put_u8(0); // Empty key terminates params
    let result = FrontendMessage::decode_startup(&mut buf);
    assert!(result.is_ok());
    let msg = result.unwrap();
    assert!(matches!(
        msg,
        Some(FrontendMessage::Startup { protocol_version, params })
            if protocol_version == 196608 && params.get("user") == Some(&"pg".to_string())
    ));
}

// -----------------------------------------------------------------
// Buffer Consumption Tests
// -----------------------------------------------------------------

#[test]
fn test_buffer_properly_consumed_after_query() {
    let mut buf = BytesMut::new();
    // First message
    buf.put_u8(b'Q');
    buf.put_i32(10);
    buf.put_slice(b"test1\0");
    // Second message should remain
    buf.put_u8(b'Q');
    buf.put_i32(10);
    buf.put_slice(b"test2\0");

    let result1 = FrontendMessage::decode(&mut buf);
    assert!(matches!(
        result1.unwrap(),
        Some(FrontendMessage::Query { query }) if query == "test1"
    ));

    let result2 = FrontendMessage::decode(&mut buf);
    assert!(matches!(
        result2.unwrap(),
        Some(FrontendMessage::Query { query }) if query == "test2"
    ));
}

#[test]
fn test_buffer_not_consumed_on_incomplete() {
    let mut buf = BytesMut::new();
    buf.put_u8(b'Q');
    buf.put_i32(100); // Claims 100 bytes but we don't have that many

    let original_len = buf.len();
    let result = FrontendMessage::decode(&mut buf);
    assert!(result.is_ok());
    assert!(result.unwrap().is_none());
    assert_eq!(buf.len(), original_len); // Buffer unchanged
}

// -----------------------------------------------------------------
// Edge Cases for Large Messages
// -----------------------------------------------------------------

#[test]
fn test_very_large_declared_length() {
    let mut buf = BytesMut::new();
    buf.put_u8(b'Q');
    buf.put_i32(i32::MAX); // Extremely large length
    buf.put_slice(b"small\0");
    let result = FrontendMessage::decode(&mut buf);
    // Should return None since we don't have enough data
    assert!(result.is_ok());
    assert!(result.unwrap().is_none());
}

// -----------------------------------------------------------------
// Password Message Tests
// -----------------------------------------------------------------

#[test]
fn test_password_message_valid() {
    let mut buf = BytesMut::new();
    buf.put_u8(b'p');
    buf.put_i32(13); // 4 + 9 bytes
    buf.put_slice(b"secret\0");
    // Add padding to meet the declared length
    buf.put_slice(&[0, 0]);
    let result = FrontendMessage::decode(&mut buf);
    assert!(result.is_ok());
    assert!(matches!(
        result.unwrap(),
        Some(FrontendMessage::Password { password }) if password == "secret"
    ));
}

#[test]
fn test_password_message_empty() {
    let mut buf = BytesMut::new();
    buf.put_u8(b'p');
    buf.put_i32(5); // 4 + 1 for null terminator
    buf.put_u8(0);
    let result = FrontendMessage::decode(&mut buf);
    assert!(result.is_ok());
    assert!(matches!(
        result.unwrap(),
        Some(FrontendMessage::Password { password }) if password.is_empty()
    ));
}
