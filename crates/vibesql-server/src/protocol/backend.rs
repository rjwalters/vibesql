//! Backend messages (server -> client)
//!
//! Protocol messages sent from the server to the client.

use std::collections::HashMap;

use bytes::{BufMut, BytesMut};

use super::{
    helpers::{encode_notice_or_error, put_cstring},
    types::{FieldDescription, PartialRowUpdate, SubscriptionUpdateType, TransactionStatus},
};

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

    // =========================================================================
    // Extended Query Protocol Response Messages
    // =========================================================================
    /// ParseComplete ('1') - Sent after successful Parse
    ParseComplete,

    /// BindComplete ('2') - Sent after successful Bind
    BindComplete,

    /// CloseComplete ('3') - Sent after successful Close
    CloseComplete,

    /// NoData ('n') - Sent by Describe when query returns no rows
    NoData,

    /// ParameterDescription ('t') - Describes prepared statement parameters
    ParameterDescription {
        /// OIDs of parameter types
        param_types: Vec<i32>,
    },

    /// PortalSuspended ('s') - Execute completed but more rows exist
    PortalSuspended,

    // =========================================================================
    // Subscription Protocol Messages (VibeSQL Extension)
    // =========================================================================
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

            // =================================================================
            // Extended Query Protocol Response Messages
            // =================================================================
            BackendMessage::ParseComplete => {
                buf.put_u8(b'1'); // ParseComplete
                buf.put_i32(4); // Length (just the length field itself)
            }

            BackendMessage::BindComplete => {
                buf.put_u8(b'2'); // BindComplete
                buf.put_i32(4);
            }

            BackendMessage::CloseComplete => {
                buf.put_u8(b'3'); // CloseComplete
                buf.put_i32(4);
            }

            BackendMessage::NoData => {
                buf.put_u8(b'n'); // NoData
                buf.put_i32(4);
            }

            BackendMessage::ParameterDescription { param_types } => {
                buf.put_u8(b't'); // ParameterDescription
                let len = 4 + 2 + (param_types.len() * 4); // length + count + OIDs
                buf.put_i32(len as i32);
                buf.put_i16(param_types.len() as i16);
                for oid in param_types {
                    buf.put_i32(*oid);
                }
            }

            BackendMessage::PortalSuspended => {
                buf.put_u8(b's'); // PortalSuspended
                buf.put_i32(4);
            }

            // =================================================================
            // Subscription Protocol Messages (VibeSQL Extension)
            // =================================================================
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
