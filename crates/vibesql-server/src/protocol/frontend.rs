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

    /// Query message
    Query { query: String },

    /// Terminate message
    Terminate,

    /// SSL request
    SSLRequest,

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
