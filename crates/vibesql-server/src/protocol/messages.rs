use bytes::{Buf, BufMut, BytesMut};
use std::collections::HashMap;
use std::io;
use thiserror::Error;

/// Wire protocol configuration for selective column updates
///
/// Sent by clients to override server-level selective update thresholds
/// on a per-subscription basis.
#[derive(Debug, Clone, PartialEq)]
pub struct SelectiveUpdatesConfig {
    /// Enable/disable selective updates for this subscription
    pub enabled: Option<bool>,
    /// Minimum columns that must change to use selective update
    /// If fewer columns change, send full row instead
    pub min_changed_columns: Option<usize>,
    /// Maximum ratio of changed columns before falling back to full row
    /// E.g., 0.5 means if >50% of columns changed, send full row instead
    pub max_changed_columns_ratio: Option<f64>,
}

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

/// Subscription update type for SubscriptionData message
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum SubscriptionUpdateType {
    Full = 0,
    DeltaInsert = 1,
    DeltaUpdate = 2,
    DeltaDelete = 3,
    /// Selective column update - only changed columns are sent
    /// Used with SubscriptionPartialData message
    SelectiveUpdate = 4,
}

/// A partial row update containing only changed columns
///
/// Used for selective column updates to reduce bandwidth when only
/// a few columns change in a wide table.
#[derive(Debug, Clone, PartialEq)]
pub struct PartialRowUpdate {
    /// Total number of columns in the full row (for bitmap sizing)
    pub total_columns: u16,
    /// Bitmap indicating which columns are present (1 bit per column)
    /// Bit 0 = column 0, Bit 1 = column 1, etc.
    /// A set bit means the column value is included in `values`
    pub column_mask: Vec<u8>,
    /// Values for columns with set bits in column_mask, in column order
    /// None = NULL, Some(bytes) = value data
    pub values: Vec<Option<Vec<u8>>>,
}

impl PartialRowUpdate {
    /// Create a new partial row update
    ///
    /// # Arguments
    /// * `total_columns` - Total number of columns in the full row
    /// * `present_columns` - Indices of columns that are present in this update
    /// * `values` - Values for the present columns, in same order as present_columns
    pub fn new(total_columns: u16, present_columns: &[u16], values: Vec<Option<Vec<u8>>>) -> Self {
        debug_assert_eq!(present_columns.len(), values.len());

        // Create bitmap
        let bitmap_bytes = (total_columns as usize).div_ceil(8);
        let mut column_mask = vec![0u8; bitmap_bytes];

        for &col_idx in present_columns {
            if (col_idx as usize) < total_columns as usize {
                let byte_idx = col_idx as usize / 8;
                let bit_idx = col_idx as usize % 8;
                column_mask[byte_idx] |= 1 << bit_idx;
            }
        }

        Self { total_columns, column_mask, values }
    }

    /// Check if a column is present in this update
    pub fn is_column_present(&self, col_idx: u16) -> bool {
        if col_idx >= self.total_columns {
            return false;
        }
        let byte_idx = col_idx as usize / 8;
        let bit_idx = col_idx as usize % 8;
        if byte_idx < self.column_mask.len() {
            (self.column_mask[byte_idx] & (1 << bit_idx)) != 0
        } else {
            false
        }
    }

    /// Get the number of present columns
    pub fn present_column_count(&self) -> usize {
        self.column_mask.iter().map(|b| b.count_ones() as usize).sum()
    }
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

    /// Subscription data (0xF2) - query result update
    SubscriptionData {
        subscription_id: [u8; 16],
        update_type: SubscriptionUpdateType,
        rows: Vec<Vec<Option<Vec<u8>>>>,
    },

    /// Subscription error (0xF3) - subscription error notification
    SubscriptionError { subscription_id: [u8; 16], message: String },

/// Subscription acknowledgment (0xF4) - confirms subscription registration
    /// Sent immediately after a subscription is registered, before initial data
    SubscriptionAck {
        subscription_id: [u8; 16],
        /// Number of table dependencies the subscription monitors
        table_count: u16,
    },

    /// Subscription partial data (0xF7) - selective column update
    ///
    /// Used for sending only changed columns in row updates, reducing bandwidth
    /// for wide tables where only a few columns change frequently.
    ///
    /// Wire format:
    /// - 1 byte: Message type (0xF7)
    /// - 4 bytes: Length (big-endian)
    /// - 16 bytes: Subscription ID
    /// - 1 byte: Update type (always SelectiveUpdate = 4)
    /// - 4 bytes: Row count (big-endian)
    /// - For each row:
    ///   - 2 bytes: Total column count (big-endian)
    ///   - N bytes: Column presence bitmap (ceil(total_columns / 8) bytes)
    ///   - For each present column (bit=1):
    ///     - 4 bytes: Value length (-1 for NULL)
    ///     - M bytes: Value data (if length >= 0)
    SubscriptionPartialData {
        subscription_id: [u8; 16],
        /// Partial row updates with column bitmaps
        rows: Vec<PartialRowUpdate>,
    },
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
    Startup { protocol_version: i32, params: HashMap<String, String> },

    /// Password message
    Password { password: String },

    /// Query message
    Query { query: String },

    /// Terminate message
    Terminate,

    /// SSL request
    SSLRequest,

    /// Subscribe message (0xF0) - subscribe to query
    /// The optional filter is a SQL WHERE clause expression applied to subscription updates.
    /// The optional selective_updates_config allows clients to override server-level selective update settings.
    Subscribe {
        query: String,
        params: Vec<Option<Vec<u8>>>,
        filter: Option<String>,
        selective_updates_config: Option<SelectiveUpdatesConfig>,
    },

    /// Unsubscribe message (0xF1) - cancel subscription
    Unsubscribe { subscription_id: [u8; 16] },

    /// Pause subscription message (0xF5) - temporarily pause updates
    SubscriptionPause { subscription_id: [u8; 16] },

    /// Resume subscription message (0xF6) - resume paused subscription
    SubscriptionResume { subscription_id: [u8; 16] },
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

            BackendMessage::BackendKeyData { process_id, secret_key } => {
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

            BackendMessage::SubscriptionData { subscription_id, update_type, rows } => {
                buf.put_u8(0xF2); // SubscriptionData

                // Calculate total length
                let mut len = 4 + 16 + 1 + 4; // length + subscription_id + update_type + row count
                for row in rows {
                    len += 2; // column count
                    for value in row {
                        len += 4; // value length
                        if let Some(v) = value {
                            len += v.len();
                        }
                    }
                }

                buf.put_i32(len as i32);
                buf.put_slice(subscription_id);
                buf.put_u8(*update_type as u8);
                buf.put_i32(rows.len() as i32);

                for row in rows {
                    buf.put_i16(row.len() as i16);
                    for value in row {
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
            }

            BackendMessage::SubscriptionError { subscription_id, message } => {
                buf.put_u8(0xF3); // SubscriptionError

                let msg_bytes = message.as_bytes();
                let len = 4 + 16 + msg_bytes.len() + 1; // length + subscription_id + message + null terminator

                buf.put_i32(len as i32);
                buf.put_slice(subscription_id);
                put_cstring(buf, message);
            }

BackendMessage::SubscriptionAck { subscription_id, table_count } => {
                buf.put_u8(0xF4); // SubscriptionAck

                let len: i32 = 4 + 16 + 2; // length + subscription_id + table_count

                buf.put_i32(len);
                buf.put_slice(subscription_id);
                buf.put_u16(*table_count);
            }

            BackendMessage::SubscriptionPartialData { subscription_id, rows } => {
                buf.put_u8(0xF7); // SubscriptionPartialData

                // Calculate total length
                // 4 (length field) + 16 (subscription_id) + 1 (update_type) + 4 (row count)
                let mut len = 4 + 16 + 1 + 4;
                for row in rows {
                    // 2 (total_columns) + bitmap bytes + values
                    len += 2;
                    len += row.column_mask.len();
                    for value in &row.values {
                        len += 4; // value length field
                        if let Some(v) = value {
                            len += v.len();
                        }
                    }
                }

                buf.put_i32(len as i32);
                buf.put_slice(subscription_id);
                buf.put_u8(SubscriptionUpdateType::SelectiveUpdate as u8);
                buf.put_i32(rows.len() as i32);

                for row in rows {
                    buf.put_i16(row.total_columns as i16);
                    buf.put_slice(&row.column_mask);
                    for value in &row.values {
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

            0xF0 => {
                // Subscribe message
                buf.advance(4); // length
                let query = read_cstring(buf)?;
                let param_count = buf.get_i16() as usize;
                let mut params = Vec::with_capacity(param_count);

                for _ in 0..param_count {
                    let param_len = buf.get_i32();
                    if param_len < 0 {
                        params.push(None);
                    } else {
                        let mut param = vec![0u8; param_len as usize];
                        buf.copy_to_slice(&mut param);
                        params.push(Some(param));
                    }
                }

                // Read optional filter expression (protocol extension)
                // If there's data remaining, read the filter length
                let filter = if buf.remaining() >= 2 {
                    let filter_len = buf.get_i16();
                    if filter_len > 0 {
                        let filter_len = filter_len as usize;
                        if buf.remaining() >= filter_len {
                            let mut filter_bytes = vec![0u8; filter_len];
                            buf.copy_to_slice(&mut filter_bytes);
                            Some(
                                String::from_utf8(filter_bytes)
                                    .map_err(|_| ProtocolError::InvalidString)?,
                            )
                        } else {
                            None // Not enough data for filter
                        }
                    } else {
                        None // No filter (length = 0 or negative)
                    }
                } else {
                    None // No filter field present (backward compatibility)
                };

                // Read optional selective updates configuration (protocol extension)
                // Format: 1 byte flags + optional values
                // Bit 0: enabled flag present
                // Bit 1: min_changed_columns present
                // Bit 2: max_changed_columns_ratio present
                let selective_updates_config = if buf.remaining() >= 1 {
                    let config_flags = buf.get_u8();
                    if config_flags != 0 {
                        let mut config = SelectiveUpdatesConfig {
                            enabled: None,
                            min_changed_columns: None,
                            max_changed_columns_ratio: None,
                        };

                        // Read enabled flag if present
                        if (config_flags & 0x01) != 0 && buf.remaining() >= 1 {
                            config.enabled = Some(buf.get_u8() != 0);
                        }

                        // Read min_changed_columns if present
                        if (config_flags & 0x02) != 0 && buf.remaining() >= 2 {
                            config.min_changed_columns = Some(buf.get_u16() as usize);
                        }

                        // Read max_changed_columns_ratio if present
                        if (config_flags & 0x04) != 0 && buf.remaining() >= 8 {
                            config.max_changed_columns_ratio = Some(buf.get_f64());
                        }

                        Some(config)
                    } else {
                        None // config_flags = 0 means no config
                    }
                } else {
                    None // No config field present (backward compatibility)
                };

                Ok(Some(FrontendMessage::Subscribe { query, params, filter, selective_updates_config }))
            }

            0xF1 => {
                // Unsubscribe message
                buf.advance(4); // length
                let mut subscription_id = [0u8; 16];
                buf.copy_to_slice(&mut subscription_id);
                Ok(Some(FrontendMessage::Unsubscribe { subscription_id }))
            }

            0xF5 => {
                // SubscriptionPause message
                buf.advance(4); // length
                let mut subscription_id = [0u8; 16];
                buf.copy_to_slice(&mut subscription_id);
                Ok(Some(FrontendMessage::SubscriptionPause { subscription_id }))
            }

            0xF6 => {
                // SubscriptionResume message
                buf.advance(4); // length
                let mut subscription_id = [0u8; 16];
                buf.copy_to_slice(&mut subscription_id);
                Ok(Some(FrontendMessage::SubscriptionResume { subscription_id }))
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

        Ok(Some(FrontendMessage::Startup { protocol_version, params }))
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
        BackendMessage::ReadyForQuery { status: TransactionStatus::Idle }.encode(&mut buf);

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

    #[test]
    fn test_subscribe_message_parsing() {
        let mut buf = BytesMut::new();
        buf.put_u8(0xF0); // Subscribe
        let mut content = BytesMut::new();
        content.put_slice(b"SELECT * FROM users\0");
        content.put_i16(0); // No params

        buf.put_i32((4 + content.len()) as i32);
        buf.extend(content);

        let msg = FrontendMessage::decode(&mut buf).unwrap();
        assert!(matches!(
            msg,
            Some(FrontendMessage::Subscribe { query, params, filter, .. })
            if query == "SELECT * FROM users" && params.is_empty() && filter.is_none()
        ));
    }

    #[test]
    fn test_subscribe_with_parameters() {
        let mut buf = BytesMut::new();
        buf.put_u8(0xF0); // Subscribe
        let mut content = BytesMut::new();
        content.put_slice(b"SELECT * FROM users WHERE id = $1\0");
        content.put_i16(1); // 1 param
        content.put_i32(5); // param length
        content.put_slice(b"12345");

        buf.put_i32((4 + content.len()) as i32);
        buf.extend(content);

        let msg = FrontendMessage::decode(&mut buf).unwrap();
        assert!(matches!(
            msg,
            Some(FrontendMessage::Subscribe { query, params, filter, .. })
            if query == "SELECT * FROM users WHERE id = $1" && params.len() == 1 && filter.is_none()
        ));
    }

    #[test]
    fn test_subscribe_with_filter() {
        let mut buf = BytesMut::new();
        buf.put_u8(0xF0); // Subscribe
        let mut content = BytesMut::new();
        content.put_slice(b"SELECT * FROM users\0");
        content.put_i16(0); // No params
        let filter_str = "status = 'active'";
        content.put_i16(filter_str.len() as i16); // Filter length
        content.put_slice(filter_str.as_bytes()); // Filter expression

        buf.put_i32((4 + content.len()) as i32);
        buf.extend(content);

        let msg = FrontendMessage::decode(&mut buf).unwrap();
        match msg {
            Some(FrontendMessage::Subscribe { query, params, filter, .. }) => {
                assert_eq!(query, "SELECT * FROM users");
                assert!(params.is_empty());
                assert_eq!(filter, Some("status = 'active'".to_string()));
            }
            _ => panic!("Expected Subscribe message"),
        }
    }

    #[test]
    fn test_subscribe_with_empty_filter() {
        let mut buf = BytesMut::new();
        buf.put_u8(0xF0); // Subscribe
        let mut content = BytesMut::new();
        content.put_slice(b"SELECT * FROM users\0");
        content.put_i16(0); // No params
        content.put_i16(0); // Filter length = 0 (no filter)

        buf.put_i32((4 + content.len()) as i32);
        buf.extend(content);

        let msg = FrontendMessage::decode(&mut buf).unwrap();
        assert!(matches!(
            msg,
            Some(FrontendMessage::Subscribe { query, params, filter, .. })
            if query == "SELECT * FROM users" && params.is_empty() && filter.is_none()
        ));
    }

    #[test]
    fn test_unsubscribe_message_parsing() {
        let mut buf = BytesMut::new();
        buf.put_u8(0xF1); // Unsubscribe
        buf.put_i32(20); // Length: 4 (length) + 16 (UUID)
        buf.put_slice(&[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]);

        let msg = FrontendMessage::decode(&mut buf).unwrap();
        assert!(matches!(
            msg,
            Some(FrontendMessage::Unsubscribe { subscription_id })
            if subscription_id == [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]
        ));
    }

    #[test]
    fn test_subscription_data_encoding() {
        let mut buf = BytesMut::new();
        let subscription_id = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16];
        let rows = vec![vec![Some(b"value1".to_vec()), Some(b"value2".to_vec())]];

        let msg = BackendMessage::SubscriptionData {
            subscription_id,
            update_type: SubscriptionUpdateType::Full,
            rows,
        };
        msg.encode(&mut buf);

        assert_eq!(buf[0], 0xF2);
        // Verify subscription_id is at bytes 5-20
        assert_eq!(&buf[5..21], subscription_id.as_ref());
    }

    #[test]
    fn test_subscription_error_encoding() {
        let mut buf = BytesMut::new();
        let subscription_id = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16];

        let msg = BackendMessage::SubscriptionError {
            subscription_id,
            message: "Query error".to_string(),
        };
        msg.encode(&mut buf);

        assert_eq!(buf[0], 0xF3);
        // Verify subscription_id is at bytes 5-20
        assert_eq!(&buf[5..21], subscription_id.as_ref());
    }

    #[test]
    fn test_subscription_ack_encoding() {
        let mut buf = BytesMut::new();
        let subscription_id = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16];

        let msg = BackendMessage::SubscriptionAck { subscription_id, table_count: 3 };
        msg.encode(&mut buf);

        assert_eq!(buf[0], 0xF4);
        // Verify length field (4 + 16 + 2 = 22)
        assert_eq!(&buf[1..5], &[0, 0, 0, 22]);
        // Verify subscription_id is at bytes 5-20
        assert_eq!(&buf[5..21], subscription_id.as_ref());
        // Verify table_count (big-endian u16)
        assert_eq!(&buf[21..23], &[0, 3]);
    }

    #[test]
    fn test_subscription_pause_parsing() {
        let mut buf = BytesMut::new();
        buf.put_u8(0xF5); // SubscriptionPause
        buf.put_i32(20); // Length: 4 (length) + 16 (UUID)
        buf.put_slice(&[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]);

        let msg = FrontendMessage::decode(&mut buf).unwrap();
        assert!(matches!(
            msg,
            Some(FrontendMessage::SubscriptionPause { subscription_id })
            if subscription_id == [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]
        ));
    }

    #[test]
    fn test_subscription_resume_parsing() {
        let mut buf = BytesMut::new();
        buf.put_u8(0xF6); // SubscriptionResume
        buf.put_i32(20); // Length: 4 (length) + 16 (UUID)
        buf.put_slice(&[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]);

        let msg = FrontendMessage::decode(&mut buf).unwrap();
        assert!(matches!(
            msg,
            Some(FrontendMessage::SubscriptionResume { subscription_id })
            if subscription_id == [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]
        ));
    }

    #[test]
    fn test_subscription_partial_data_encoding() {
        let mut buf = BytesMut::new();
        let subscription_id = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16];

        // Create a partial row update with 4 columns, columns 0 and 2 present
        let partial_row = PartialRowUpdate::new(
            4,
            &[0, 2],
            vec![Some(b"id1".to_vec()), Some(b"value".to_vec())],
        );

        let msg = BackendMessage::SubscriptionPartialData {
            subscription_id,
            rows: vec![partial_row],
        };
        msg.encode(&mut buf);

        // Verify message type (0xF7)
        assert_eq!(buf[0], 0xF7);

        // Verify subscription_id is at bytes 5-20
        assert_eq!(&buf[5..21], subscription_id.as_ref());

        // Verify update type is SelectiveUpdate (4)
        assert_eq!(buf[21], 4);

        // Verify row count is 1
        let row_count = i32::from_be_bytes([buf[22], buf[23], buf[24], buf[25]]);
        assert_eq!(row_count, 1);

        // Verify total columns is 4
        let total_cols = i16::from_be_bytes([buf[26], buf[27]]);
        assert_eq!(total_cols, 4);

        // Verify column bitmap (1 byte for 4 columns)
        // Columns 0 and 2: binary 0101 = 5
        assert_eq!(buf[28], 0b00000101);
    }

    #[test]
    fn test_subscription_partial_data_encoding_with_null() {
        let mut buf = BytesMut::new();
        let subscription_id = [0u8; 16];

        // Create a partial row update with NULL value
        let partial_row = PartialRowUpdate::new(
            3,
            &[0, 1],
            vec![Some(b"1".to_vec()), None], // Column 1 is NULL
        );

        let msg = BackendMessage::SubscriptionPartialData {
            subscription_id,
            rows: vec![partial_row],
        };
        msg.encode(&mut buf);

        assert_eq!(buf[0], 0xF7);

        // After subscription_id (16 bytes), update_type (1 byte), row_count (4 bytes)
        // total_columns (2 bytes), column_mask (1 byte for 3 columns)
        // First value: length (4) + data (1)
        // Second value: length (-1) for NULL

        // Find the position of the NULL value length (-1)
        // Position: 1 (type) + 4 (len) + 16 (id) + 1 (update_type) + 4 (row_count)
        //         + 2 (total_cols) + 1 (bitmap) + 4 (val1_len) + 1 (val1_data) = 34
        let null_pos = 34;
        let null_len = i32::from_be_bytes([buf[null_pos], buf[null_pos + 1], buf[null_pos + 2], buf[null_pos + 3]]);
        assert_eq!(null_len, -1);
    }

    #[test]
    fn test_partial_row_update_new() {
        // Test with 16 columns to verify multi-byte bitmap
        let partial = PartialRowUpdate::new(
            16,
            &[0, 8, 15],
            vec![Some(b"a".to_vec()), Some(b"b".to_vec()), Some(b"c".to_vec())],
        );

        assert_eq!(partial.total_columns, 16);
        assert_eq!(partial.column_mask.len(), 2); // ceil(16/8) = 2 bytes

        // Byte 0: bit 0 set (column 0) = 0x01
        // Byte 1: bit 0 set (column 8), bit 7 set (column 15) = 0x81
        assert_eq!(partial.column_mask[0], 0b00000001);
        assert_eq!(partial.column_mask[1], 0b10000001);

        assert!(partial.is_column_present(0));
        assert!(!partial.is_column_present(1));
        assert!(partial.is_column_present(8));
        assert!(partial.is_column_present(15));
        assert!(!partial.is_column_present(16)); // Out of range
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

        // -----------------------------------------------------------------
        // SelectiveUpdatesConfig Tests
        // -----------------------------------------------------------------

        #[test]
        fn test_subscribe_with_selective_updates_config_full() {
            // Test parsing Subscribe with all config fields set
            let mut buf = BytesMut::new();
            buf.put_u8(0xF0); // Subscribe message type
            
            // Build the message body first to calculate length
            let mut body = BytesMut::new();
            
            // Query
            body.put_slice(b"SELECT * FROM test\0");
            
            // Parameters (no params)
            body.put_i16(0);
            
            // Filter (none)
            body.put_i16(0);
            
            // Selective updates config
            body.put_u8(0x07); // All three flags set (0b111)
            body.put_u8(1); // enabled = true
            body.put_u16(5); // min_changed_columns = 5
            body.put_f64(0.75); // max_changed_columns_ratio = 0.75
            
            // Write length (4 bytes for length field itself + body)
            buf.put_i32((4 + body.len()) as i32);
            buf.put_slice(&body);
            
            let result = FrontendMessage::decode(&mut buf);
            assert!(result.is_ok());
            
            let msg = result.unwrap();
            assert!(matches!(msg, Some(FrontendMessage::Subscribe { .. })));
            
            if let Some(FrontendMessage::Subscribe { selective_updates_config, .. }) = msg {
                assert!(selective_updates_config.is_some());
                let config = selective_updates_config.unwrap();
                assert_eq!(config.enabled, Some(true));
                assert_eq!(config.min_changed_columns, Some(5));
                assert_eq!(config.max_changed_columns_ratio, Some(0.75));
            } else {
                panic!("Expected Subscribe message");
            }
        }

        #[test]
        fn test_subscribe_with_partial_selective_config_enabled_only() {
            // Test parsing Subscribe with only enabled flag set
            let mut buf = BytesMut::new();
            buf.put_u8(0xF0); // Subscribe message type
            
            let mut body = BytesMut::new();
            body.put_slice(b"SELECT * FROM test\0");
            body.put_i16(0); // no params
            body.put_i16(0); // no filter
            
            body.put_u8(0x01); // Only enabled flag set (0b001)
            body.put_u8(1); // enabled = true
            
            buf.put_i32((4 + body.len()) as i32);
            buf.put_slice(&body);
            
            let result = FrontendMessage::decode(&mut buf);
            assert!(result.is_ok());
            
            if let Some(FrontendMessage::Subscribe { selective_updates_config, .. }) = result.unwrap() {
                assert!(selective_updates_config.is_some());
                let config = selective_updates_config.unwrap();
                assert_eq!(config.enabled, Some(true));
                assert_eq!(config.min_changed_columns, None);
                assert_eq!(config.max_changed_columns_ratio, None);
            } else {
                panic!("Expected Subscribe message with config");
            }
        }

        #[test]
        fn test_subscribe_with_partial_selective_config_min_columns_only() {
            // Test parsing Subscribe with only min_changed_columns flag set
            let mut buf = BytesMut::new();
            buf.put_u8(0xF0); // Subscribe message type
            
            let mut body = BytesMut::new();
            body.put_slice(b"SELECT * FROM test\0");
            body.put_i16(0); // no params
            body.put_i16(0); // no filter
            
            body.put_u8(0x02); // Only min_changed_columns flag set (0b010)
            body.put_u16(10); // min_changed_columns = 10
            
            buf.put_i32((4 + body.len()) as i32);
            buf.put_slice(&body);
            
            let result = FrontendMessage::decode(&mut buf);
            assert!(result.is_ok());
            
            if let Some(FrontendMessage::Subscribe { selective_updates_config, .. }) = result.unwrap() {
                assert!(selective_updates_config.is_some());
                let config = selective_updates_config.unwrap();
                assert_eq!(config.enabled, None);
                assert_eq!(config.min_changed_columns, Some(10));
                assert_eq!(config.max_changed_columns_ratio, None);
            } else {
                panic!("Expected Subscribe message with config");
            }
        }

        #[test]
        fn test_subscribe_with_partial_selective_config_max_ratio_only() {
            // Test parsing Subscribe with only max_changed_columns_ratio flag set
            let mut buf = BytesMut::new();
            buf.put_u8(0xF0); // Subscribe message type
            
            let mut body = BytesMut::new();
            body.put_slice(b"SELECT * FROM test\0");
            body.put_i16(0); // no params
            body.put_i16(0); // no filter
            
            body.put_u8(0x04); // Only max_changed_columns_ratio flag set (0b100)
            body.put_f64(0.5); // max_changed_columns_ratio = 0.5
            
            buf.put_i32((4 + body.len()) as i32);
            buf.put_slice(&body);
            
            let result = FrontendMessage::decode(&mut buf);
            assert!(result.is_ok());
            
            if let Some(FrontendMessage::Subscribe { selective_updates_config, .. }) = result.unwrap() {
                assert!(selective_updates_config.is_some());
                let config = selective_updates_config.unwrap();
                assert_eq!(config.enabled, None);
                assert_eq!(config.min_changed_columns, None);
                assert_eq!(config.max_changed_columns_ratio, Some(0.5));
            } else {
                panic!("Expected Subscribe message with config");
            }
        }

        #[test]
        fn test_subscribe_with_selective_config_zero_flags() {
            // Test that config_flags = 0 results in None config
            let mut buf = BytesMut::new();
            buf.put_u8(0xF0); // Subscribe message type
            
            let mut body = BytesMut::new();
            body.put_slice(b"SELECT * FROM test\0");
            body.put_i16(0); // no params
            body.put_i16(0); // no filter
            body.put_u8(0x00); // config_flags = 0 (no config)
            
            buf.put_i32((4 + body.len()) as i32);
            buf.put_slice(&body);
            
            let result = FrontendMessage::decode(&mut buf);
            assert!(result.is_ok());
            
            if let Some(FrontendMessage::Subscribe { selective_updates_config, .. }) = result.unwrap() {
                assert!(selective_updates_config.is_none());
            } else {
                panic!("Expected Subscribe message");
            }
        }

        #[test]
        fn test_subscribe_without_selective_config_field() {
            // Test backward compatibility: Subscribe without config field present
            let mut buf = BytesMut::new();
            buf.put_u8(0xF0); // Subscribe message type
            
            let mut body = BytesMut::new();
            body.put_slice(b"SELECT * FROM test\0");
            body.put_i16(0); // no params
            body.put_i16(0); // no filter
            // No config field at all
            
            buf.put_i32((4 + body.len()) as i32);
            buf.put_slice(&body);
            
            let result = FrontendMessage::decode(&mut buf);
            assert!(result.is_ok());
            
            if let Some(FrontendMessage::Subscribe { selective_updates_config, .. }) = result.unwrap() {
                assert!(selective_updates_config.is_none());
            } else {
                panic!("Expected Subscribe message");
            }
        }

        #[test]
        fn test_subscribe_with_config_disabled() {
            // Test parsing with enabled = false
            let mut buf = BytesMut::new();
            buf.put_u8(0xF0); // Subscribe message type
            
            let mut body = BytesMut::new();
            body.put_slice(b"SELECT * FROM test\0");
            body.put_i16(0); // no params
            body.put_i16(0); // no filter
            
            body.put_u8(0x01); // enabled flag set
            body.put_u8(0); // enabled = false
            
            buf.put_i32((4 + body.len()) as i32);
            buf.put_slice(&body);
            
            let result = FrontendMessage::decode(&mut buf);
            assert!(result.is_ok());
            
            if let Some(FrontendMessage::Subscribe { selective_updates_config, .. }) = result.unwrap() {
                assert!(selective_updates_config.is_some());
                let config = selective_updates_config.unwrap();
                assert_eq!(config.enabled, Some(false));
            } else {
                panic!("Expected Subscribe message with config");
            }
        }

        #[test]
        fn test_subscribe_with_combined_flags() {
            // Test parsing with enabled and min_changed_columns flags
            let mut buf = BytesMut::new();
            buf.put_u8(0xF0); // Subscribe message type
            
            let mut body = BytesMut::new();
            body.put_slice(b"SELECT * FROM test\0");
            body.put_i16(0); // no params
            body.put_i16(0); // no filter
            
            body.put_u8(0x03); // enabled and min_changed_columns flags (0b011)
            body.put_u8(1); // enabled = true
            body.put_u16(3); // min_changed_columns = 3
            
            buf.put_i32((4 + body.len()) as i32);
            buf.put_slice(&body);
            
            let result = FrontendMessage::decode(&mut buf);
            assert!(result.is_ok());
            
            if let Some(FrontendMessage::Subscribe { selective_updates_config, .. }) = result.unwrap() {
                assert!(selective_updates_config.is_some());
                let config = selective_updates_config.unwrap();
                assert_eq!(config.enabled, Some(true));
                assert_eq!(config.min_changed_columns, Some(3));
                assert_eq!(config.max_changed_columns_ratio, None);
            } else {
                panic!("Expected Subscribe message with config");
            }
        }
    }
}
