//! Frontend messages (client -> server)
//!
//! Protocol messages sent from the client to the server.

use std::collections::HashMap;

use bytes::{Buf, BytesMut};

use super::helpers::read_cstring;
use super::types::{ProtocolError, SelectiveUpdatesConfig};

/// Frontend message types (client -> server)
#[derive(Debug, Clone, PartialEq)]
pub enum FrontendMessage {
    /// Startup message
    Startup { protocol_version: i32, params: HashMap<String, String> },

    /// Password message
    Password { password: String },

    /// Query message (simple query protocol)
    Query { query: String },

    /// Terminate message
    Terminate,

    /// SSL request
    SSLRequest,

    // =========================================================================
    // Extended Query Protocol Messages
    // =========================================================================
    /// Parse message ('P') - Prepare a statement
    ///
    /// Creates a prepared statement from a SQL query string. The statement can
    /// optionally specify parameter types (OIDs). An empty name creates an
    /// unnamed prepared statement.
    Parse {
        /// Name of the prepared statement (empty string for unnamed)
        name: String,
        /// SQL query with optional $1, $2, ... parameter placeholders
        query: String,
        /// OIDs of parameter types (0 means unspecified, let server infer)
        param_types: Vec<i32>,
    },

    /// Bind message ('B') - Bind parameters to a prepared statement
    ///
    /// Creates a portal by binding parameter values to a prepared statement.
    /// An empty portal name creates an unnamed portal.
    Bind {
        /// Name of the destination portal (empty string for unnamed)
        portal: String,
        /// Name of the source prepared statement (empty string for unnamed)
        statement: String,
        /// Format codes for parameters (0=text, 1=binary)
        /// If empty, all parameters use text format
        /// If one element, it applies to all parameters
        /// Otherwise, one per parameter
        param_formats: Vec<i16>,
        /// Parameter values (None for NULL)
        param_values: Vec<Option<Vec<u8>>>,
        /// Format codes for result columns
        /// Same rules as param_formats
        result_formats: Vec<i16>,
    },

    /// Describe message ('D') - Get description of prepared statement or portal
    ///
    /// Returns parameter types (for statement) or row description (for portal).
    Describe {
        /// 'S' for prepared statement, 'P' for portal
        target_type: u8,
        /// Name of the statement or portal (empty string for unnamed)
        name: String,
    },

    /// Execute message ('E') - Execute a bound portal
    ///
    /// Executes a portal and returns rows. Use max_rows=0 for unlimited.
    Execute {
        /// Name of the portal (empty string for unnamed)
        portal: String,
        /// Maximum number of rows to return (0 = unlimited)
        max_rows: i32,
    },

    /// Sync message ('S') - Synchronization point
    ///
    /// Marks the end of an extended query sequence. The server will respond
    /// with ReadyForQuery and close any implicit transaction.
    Sync,

    /// Flush message ('H') - Flush output buffer
    ///
    /// Requests the server to send any pending output immediately.
    Flush,

    /// Close message ('C') - Close a prepared statement or portal
    ///
    /// Closes and deallocates a named statement or portal.
    Close {
        /// 'S' for prepared statement, 'P' for portal
        target_type: u8,
        /// Name of the statement or portal to close
        name: String,
    },

    // =========================================================================
    // Subscription Protocol Messages (VibeSQL Extension)
    // =========================================================================
    /// Subscribe message (0xF0) - subscribe to query
    /// The optional filter is a SQL WHERE clause expression applied to subscription updates.
    /// The optional selective_updates_config allows clients to override server-level selective
    /// update settings.
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

            // =================================================================
            // Extended Query Protocol Messages
            // =================================================================
            b'P' => {
                // Parse message - prepare a statement
                buf.advance(4); // length
                let name = read_cstring(buf)?;
                let query = read_cstring(buf)?;
                let param_count = buf.get_i16() as usize;
                let mut param_types = Vec::with_capacity(param_count);
                for _ in 0..param_count {
                    param_types.push(buf.get_i32());
                }
                Ok(Some(FrontendMessage::Parse { name, query, param_types }))
            }

            b'B' => {
                // Bind message - bind parameters to a prepared statement
                buf.advance(4); // length
                let portal = read_cstring(buf)?;
                let statement = read_cstring(buf)?;

                // Parameter format codes
                let format_count = buf.get_i16() as usize;
                let mut param_formats = Vec::with_capacity(format_count);
                for _ in 0..format_count {
                    param_formats.push(buf.get_i16());
                }

                // Parameter values
                let param_count = buf.get_i16() as usize;
                let mut param_values = Vec::with_capacity(param_count);
                for _ in 0..param_count {
                    let value_len = buf.get_i32();
                    if value_len < 0 {
                        param_values.push(None); // NULL
                    } else {
                        let mut value = vec![0u8; value_len as usize];
                        buf.copy_to_slice(&mut value);
                        param_values.push(Some(value));
                    }
                }

                // Result format codes
                let result_format_count = buf.get_i16() as usize;
                let mut result_formats = Vec::with_capacity(result_format_count);
                for _ in 0..result_format_count {
                    result_formats.push(buf.get_i16());
                }

                Ok(Some(FrontendMessage::Bind {
                    portal,
                    statement,
                    param_formats,
                    param_values,
                    result_formats,
                }))
            }

            b'D' => {
                // Describe message - get description of statement or portal
                buf.advance(4); // length
                let target_type = buf.get_u8();
                let name = read_cstring(buf)?;
                Ok(Some(FrontendMessage::Describe { target_type, name }))
            }

            b'E' => {
                // Execute message - execute a portal
                buf.advance(4); // length
                let portal = read_cstring(buf)?;
                let max_rows = buf.get_i32();
                Ok(Some(FrontendMessage::Execute { portal, max_rows }))
            }

            b'S' => {
                // Sync message - end of extended query sequence
                buf.advance(4); // length
                Ok(Some(FrontendMessage::Sync))
            }

            b'H' => {
                // Flush message - flush output buffer
                buf.advance(4); // length
                Ok(Some(FrontendMessage::Flush))
            }

            b'C' => {
                // Close message - close statement or portal
                buf.advance(4); // length
                let target_type = buf.get_u8();
                let name = read_cstring(buf)?;
                Ok(Some(FrontendMessage::Close { target_type, name }))
            }

            // =================================================================
            // Subscription Protocol Messages (VibeSQL Extension)
            // =================================================================
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

                Ok(Some(FrontendMessage::Subscribe {
                    query,
                    params,
                    filter,
                    selective_updates_config,
                }))
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
