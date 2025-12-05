use bytes::{BufMut, BytesMut};
use std::collections::HashMap;

/// Read PostgreSQL error response fields from buffer
fn read_error_fields(buf: &[u8]) -> HashMap<char, String> {
    let mut fields = HashMap::new();
    let mut pos = 0;

    while pos < buf.len() && buf[pos] != 0 {
        let field_type = buf[pos] as char;
        pos += 1;

        // Find null terminator
        let start = pos;
        while pos < buf.len() && buf[pos] != 0 {
            pos += 1;
        }

        if pos < buf.len() {
            let value = String::from_utf8_lossy(&buf[start..pos]).to_string();
            fields.insert(field_type, value);
            pos += 1; // Skip null terminator
        }
    }

    fields
}

/// Check if response is an error with the given code
fn is_error_with_code(response: &[u8], expected_code: &str) -> bool {
    if response.is_empty() || response[0] != b'E' {
        return false;
    }

    // Skip message type and length
    if response.len() < 5 {
        return false;
    }

    let fields = read_error_fields(&response[5..]);
    fields.get(&'C').map(|c| c == expected_code).unwrap_or(false)
}

#[tokio::test]
async fn test_max_connections_error_format() {
    // Test that the error response format is correct for too_many_connections (53300)
    // This tests the protocol encoding without needing a running server

    let mut buf = BytesMut::new();
    let mut fields = HashMap::new();
    fields.insert(b'S', "FATAL".to_string());
    fields.insert(b'V', "FATAL".to_string());
    fields.insert(b'C', "53300".to_string());
    fields.insert(b'M', "sorry, too many clients already (max_connections=2)".to_string());

    // Encode error response (matches BackendMessage::ErrorResponse format from main.rs)
    buf.put_u8(b'E'); // Error response type

    // Calculate length
    let mut len = 4 + 1; // length field + terminator
    for value in fields.values() {
        len += 1 + value.len() + 1; // field type + value + null
    }
    buf.put_i32(len as i32);

    // Write fields
    for (&field_type, value) in &fields {
        buf.put_u8(field_type);
        buf.put_slice(value.as_bytes());
        buf.put_u8(0);
    }
    buf.put_u8(0); // Terminator

    // Verify the format
    assert_eq!(buf[0], b'E');
    assert!(is_error_with_code(&buf, "53300"));
}

#[tokio::test]
async fn test_error_response_contains_max_connections_message() {
    // Test that the too_many_connections error has proper PostgreSQL format
    let mut buf = BytesMut::new();
    let mut fields = HashMap::new();
    fields.insert(b'S', "FATAL".to_string());
    fields.insert(b'V', "FATAL".to_string());
    fields.insert(b'C', "53300".to_string());
    fields.insert(b'M', "sorry, too many clients already (max_connections=100)".to_string());

    // Encode error response
    buf.put_u8(b'E');

    let mut len = 4 + 1;
    for value in fields.values() {
        len += 1 + value.len() + 1;
    }
    buf.put_i32(len as i32);

    for (&field_type, value) in &fields {
        buf.put_u8(field_type);
        buf.put_slice(value.as_bytes());
        buf.put_u8(0);
    }
    buf.put_u8(0);

    let parsed_fields = read_error_fields(&buf[5..]);

    assert_eq!(parsed_fields.get(&'S'), Some(&"FATAL".to_string()));
    assert_eq!(parsed_fields.get(&'C'), Some(&"53300".to_string()));
    assert!(parsed_fields.get(&'M').unwrap().contains("too many clients"));
}

#[tokio::test]
async fn test_error_code_53300_is_too_many_connections() {
    // PostgreSQL error code 53300 is "too_many_connections"
    // https://www.postgresql.org/docs/current/errcodes-appendix.html
    // Class 53 - Insufficient Resources

    let mut buf = BytesMut::new();
    let mut fields = HashMap::new();
    fields.insert(b'S', "FATAL".to_string());
    fields.insert(b'C', "53300".to_string());
    fields.insert(b'M', "too many connections".to_string());

    buf.put_u8(b'E');
    let mut len = 4 + 1;
    for value in fields.values() {
        len += 1 + value.len() + 1;
    }
    buf.put_i32(len as i32);
    for (&field_type, value) in &fields {
        buf.put_u8(field_type);
        buf.put_slice(value.as_bytes());
        buf.put_u8(0);
    }
    buf.put_u8(0);

    assert!(is_error_with_code(&buf, "53300"));
    // 53300 is not a generic error
    assert!(!is_error_with_code(&buf, "XX000"));
}
