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
    #[allow(dead_code)]
    MessageTooShort,

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
        // Check if we have enough bytes for the header
        if buf.len() < 5 {
            return Ok(None);
        }

        // Peek at message type
        let msg_type = buf[0];

        // Get message length (excluding type byte, including length field itself)
        let len = i32::from_be_bytes([buf[1], buf[2], buf[3], buf[4]]) as usize;

        // Check if we have the full message
        if buf.len() < 1 + len {
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

        let len = i32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]) as usize;

        if buf.len() < len {
            return Ok(None);
        }

        buf.advance(4); // length

        let protocol_version = buf.get_i32();

        // Special case: SSL request
        if protocol_version == 80877103 {
            return Ok(Some(FrontendMessage::SSLRequest));
        }

        // Read parameters
        let mut params = HashMap::new();
        loop {
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
}
