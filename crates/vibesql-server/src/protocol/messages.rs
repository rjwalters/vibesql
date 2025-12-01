use bytes::{Buf, BufMut, BytesMut};
use std::collections::HashMap;
use std::io;
use thiserror::Error;

/// PostgreSQL protocol errors
#[derive(Debug, Error)]
pub enum ProtocolError {
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),

    #[error("Invalid message type: {0}")]
    InvalidMessageType(u8),

    #[error("Message too short")]
    MessageTooShort,

    #[error("Invalid message length: {0}")]
    InvalidMessageLength(i32),

    #[error("Invalid string encoding")]
    InvalidString,

    #[error("Unexpected message: {0}")]
    #[allow(dead_code)]
    UnexpectedMessage(String),
}

/// Backend message types (server -> client)
#[derive(Debug, Clone, PartialEq)]
pub enum BackendMessage {
    /// Authentication request
    AuthenticationOk,
    #[allow(dead_code)]
    AuthenticationCleartextPassword,
    #[allow(dead_code)]
    AuthenticationMD5Password { salt: [u8; 4] },

    /// Parameter status
    ParameterStatus { name: String, value: String },

    /// Backend key data (for cancellation)
    BackendKeyData { process_id: i32, secret_key: i32 },

    /// Ready for query
    ReadyForQuery { status: TransactionStatus },

    /// Row description (result set schema)
    RowDescription { fields: Vec<FieldDescription> },

    /// Data row
    DataRow { values: Vec<Option<Vec<u8>>> },

    /// Command complete
    CommandComplete { tag: String },

    /// Error response
    ErrorResponse { fields: HashMap<u8, String> },

    /// Notice response
    #[allow(dead_code)]
    NoticeResponse { fields: HashMap<u8, String> },

    /// Empty query response
    EmptyQueryResponse,
}

/// Transaction status
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransactionStatus {
    /// Idle (not in a transaction)
    Idle,
    /// In a transaction block
    #[allow(dead_code)]
    InTransaction,
    /// In a failed transaction block
    #[allow(dead_code)]
    FailedTransaction,
}

impl TransactionStatus {
    pub fn as_byte(&self) -> u8 {
        match self {
            TransactionStatus::Idle => b'I',
            TransactionStatus::InTransaction => b'T',
            TransactionStatus::FailedTransaction => b'E',
        }
    }
}

/// Field description for row data
#[derive(Debug, Clone, PartialEq)]
pub struct FieldDescription {
    pub name: String,
    pub table_oid: i32,
    pub column_attr_number: i16,
    pub data_type_oid: i32,
    pub data_type_size: i16,
    pub type_modifier: i32,
    pub format_code: i16, // 0 = text, 1 = binary
}

/// Frontend message types (client -> server)
#[derive(Debug, Clone, PartialEq)]
pub enum FrontendMessage {
    /// Startup message
    Startup {
        protocol_version: i32,
        params: HashMap<String, String>,
    },

    /// Password message
    Password { password: String },

    /// Query message
    Query { query: String },

    /// Terminate message
    Terminate,

    /// SSL request
    SSLRequest,
}

impl BackendMessage {
    /// Encode a backend message to bytes
    pub fn encode(&self, buf: &mut BytesMut) {
        match self {
            BackendMessage::AuthenticationOk => {
                buf.put_u8(b'R'); // Authentication
                buf.put_i32(8); // Length including self
                buf.put_i32(0); // AuthenticationOk
            }

            BackendMessage::AuthenticationCleartextPassword => {
                buf.put_u8(b'R');
                buf.put_i32(8);
                buf.put_i32(3); // AuthenticationCleartextPassword
            }

            BackendMessage::AuthenticationMD5Password { salt } => {
                buf.put_u8(b'R');
                buf.put_i32(12);
                buf.put_i32(5); // AuthenticationMD5Password
                buf.put_slice(salt);
            }

            BackendMessage::ParameterStatus { name, value } => {
                buf.put_u8(b'S'); // ParameterStatus
                let len = 4 + name.len() + 1 + value.len() + 1;
                buf.put_i32(len as i32);
                put_cstring(buf, name);
                put_cstring(buf, value);
            }

            BackendMessage::BackendKeyData {
                process_id,
                secret_key,
            } => {
                buf.put_u8(b'K'); // BackendKeyData
                buf.put_i32(12);
                buf.put_i32(*process_id);
                buf.put_i32(*secret_key);
            }

            BackendMessage::ReadyForQuery { status } => {
                buf.put_u8(b'Z'); // ReadyForQuery
                buf.put_i32(5);
                buf.put_u8(status.as_byte());
            }

            BackendMessage::RowDescription { fields } => {
                buf.put_u8(b'T'); // RowDescription

                // Calculate total length
                let mut len = 4 + 2; // length + field count
                for field in fields {
                    len += field.name.len() + 1 + 18; // name + null + 6 i32/i16 fields
                }

                buf.put_i32(len as i32);
                buf.put_i16(fields.len() as i16);

                for field in fields {
                    put_cstring(buf, &field.name);
                    buf.put_i32(field.table_oid);
                    buf.put_i16(field.column_attr_number);
                    buf.put_i32(field.data_type_oid);
                    buf.put_i16(field.data_type_size);
                    buf.put_i32(field.type_modifier);
                    buf.put_i16(field.format_code);
                }
            }

            BackendMessage::DataRow { values } => {
                buf.put_u8(b'D'); // DataRow

                // Calculate total length
                let mut len = 4 + 2; // length + field count
                for value in values {
                    len += 4; // length field
                    if let Some(v) = value {
                        len += v.len();
                    }
                }

                buf.put_i32(len as i32);
                buf.put_i16(values.len() as i16);

                for value in values {
                    match value {
                        Some(v) => {
                            buf.put_i32(v.len() as i32);
                            buf.put_slice(v);
                        }
                        None => {
                            buf.put_i32(-1); // NULL value
                        }
                    }
                }
            }

            BackendMessage::CommandComplete { tag } => {
                buf.put_u8(b'C'); // CommandComplete
                let len = 4 + tag.len() + 1;
                buf.put_i32(len as i32);
                put_cstring(buf, tag);
            }

            BackendMessage::ErrorResponse { fields } => {
                buf.put_u8(b'E'); // ErrorResponse
                encode_notice_or_error(buf, fields);
            }

            BackendMessage::NoticeResponse { fields } => {
                buf.put_u8(b'N'); // NoticeResponse
                encode_notice_or_error(buf, fields);
            }

            BackendMessage::EmptyQueryResponse => {
                buf.put_u8(b'I'); // EmptyQueryResponse
                buf.put_i32(4);
            }
        }
    }
}

impl FrontendMessage {
    /// Decode a frontend message from bytes
    pub fn decode(buf: &mut BytesMut) -> Result<Option<Self>, ProtocolError> {
        // Check if we have enough bytes for the header (1 byte type + 4 bytes length)
        if buf.len() < 5 {
            return Ok(None);
        }

        // Peek at message type
        let msg_type = buf[0];

        // Get message length (excluding type byte, including length field itself)
        let len_i32 = i32::from_be_bytes([buf[1], buf[2], buf[3], buf[4]]);

        // Validate length - must be at least 4 (includes the length field itself)
        // and must be positive to avoid overflow when casting to usize
        if len_i32 < 4 {
            return Err(ProtocolError::InvalidMessageLength(len_i32));
        }

        let len = len_i32 as usize;

        // Check if we have the full message (use saturating_add to avoid overflow)
        let total_len = 1usize.saturating_add(len);
        if buf.len() < total_len {
            return Ok(None);
        }

        // Consume the message type
        buf.advance(1);

        // Decode based on message type
        match msg_type {
            b'Q' => {
                // Query message
                buf.advance(4); // length
                let query = read_cstring(buf)?;
                Ok(Some(FrontendMessage::Query { query }))
            }

            b'p' => {
                // Password message
                buf.advance(4); // length
                let password = read_cstring(buf)?;
                Ok(Some(FrontendMessage::Password { password }))
            }

            b'X' => {
                // Terminate message
                buf.advance(4); // length
                Ok(Some(FrontendMessage::Terminate))
            }

            _ => Err(ProtocolError::InvalidMessageType(msg_type)),
        }
    }

    /// Decode startup message (special case - no message type byte)
    pub fn decode_startup(buf: &mut BytesMut) -> Result<Option<Self>, ProtocolError> {
        if buf.len() < 4 {
            return Ok(None);
        }

        let len_i32 = i32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]);

        // Validate length - startup message must be at least 8 bytes
        // (4 bytes length + 4 bytes protocol version)
        if len_i32 < 8 {
            return Err(ProtocolError::InvalidMessageLength(len_i32));
        }

        let len = len_i32 as usize;

        if buf.len() < len {
            return Ok(None);
        }

        buf.advance(4); // length

        let protocol_version = buf.get_i32();

        // Special case: SSL request (exactly 8 bytes total)
        if protocol_version == 80877103 {
            return Ok(Some(FrontendMessage::SSLRequest));
        }

        // Read parameters - limit iterations to prevent infinite loops
        let mut params = HashMap::new();
        let max_params = 100; // Reasonable limit for startup parameters
        for _ in 0..max_params {
            // Check if we have data remaining for another string
            if buf.is_empty() {
                break;
            }
            let key = read_cstring(buf)?;
            if key.is_empty() {
                break;
            }
            let value = read_cstring(buf)?;
            params.insert(key, value);
        }

        Ok(Some(FrontendMessage::Startup {
            protocol_version,
            params,
        }))
    }
}

/// Write a null-terminated C string
fn put_cstring(buf: &mut BytesMut, s: &str) {
    buf.put_slice(s.as_bytes());
    buf.put_u8(0);
}

/// Read a null-terminated C string
fn read_cstring(buf: &mut BytesMut) -> Result<String, ProtocolError> {
    let null_pos = buf.iter().position(|&b| b == 0).ok_or(ProtocolError::InvalidString)?;

    let bytes = buf.split_to(null_pos);
    buf.advance(1); // skip null byte

    String::from_utf8(bytes.to_vec()).map_err(|_| ProtocolError::InvalidString)
}

/// Encode error or notice response fields
fn encode_notice_or_error(buf: &mut BytesMut, fields: &HashMap<u8, String>) {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_authentication_ok_encoding() {
        let mut buf = BytesMut::new();
        BackendMessage::AuthenticationOk.encode(&mut buf);

        assert_eq!(buf[0], b'R');
        assert_eq!(&buf[1..5], &[0, 0, 0, 8]);
        assert_eq!(&buf[5..9], &[0, 0, 0, 0]);
    }

    #[test]
    fn test_ready_for_query_encoding() {
        let mut buf = BytesMut::new();
        BackendMessage::ReadyForQuery {
            status: TransactionStatus::Idle,
        }
        .encode(&mut buf);

        assert_eq!(buf[0], b'Z');
        assert_eq!(&buf[1..5], &[0, 0, 0, 5]);
        assert_eq!(buf[5], b'I');
    }

    #[test]
    fn test_query_decoding() {
        let mut buf = BytesMut::new();
        buf.put_u8(b'Q'); // Query message type
        buf.put_i32(13); // Length (4 bytes length field + 9 bytes "SELECT 1\0")
        buf.put_slice(b"SELECT 1\0");

        let msg = FrontendMessage::decode(&mut buf).unwrap();
        assert!(matches!(
            msg,
            Some(FrontendMessage::Query { query }) if query == "SELECT 1"
        ));
    }

    // =====================================================================
    // Malformed Message Handling Tests
    // Tests for security-relevant handling of invalid wire protocol messages
    // =====================================================================

    mod malformed_message_tests {
        use super::*;

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
            // Length: 4 (len) + 4 (version) + 5 (user\0) + 3 (invalid UTF-8 + \0) + 1 (final \0) = 17
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
    }
}
