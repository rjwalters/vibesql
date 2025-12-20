//! SelectiveUpdatesConfig parsing tests
//!
//! Tests for parsing Subscribe messages with selective updates configuration.

use bytes::{BufMut, BytesMut};

use crate::protocol::FrontendMessage;

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
