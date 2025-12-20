//! Basic encoding and decoding tests

use bytes::{BufMut, BytesMut};

use crate::protocol::{
    BackendMessage, FrontendMessage, PartialRowUpdate, SubscriptionUpdateType, TransactionStatus,
};

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

    let msg =
        BackendMessage::SubscriptionError { subscription_id, message: "Query error".to_string() };
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
    let partial_row =
        PartialRowUpdate::new(4, &[0, 2], vec![Some(b"id1".to_vec()), Some(b"value".to_vec())]);

    let msg = BackendMessage::SubscriptionPartialData { subscription_id, rows: vec![partial_row] };
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

    let msg = BackendMessage::SubscriptionPartialData { subscription_id, rows: vec![partial_row] };
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
    let null_len = i32::from_be_bytes([
        buf[null_pos],
        buf[null_pos + 1],
        buf[null_pos + 2],
        buf[null_pos + 3],
    ]);
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
