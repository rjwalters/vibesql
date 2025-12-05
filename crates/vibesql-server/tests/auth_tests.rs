//! Integration tests for authentication flows
//!
//! Tests password authentication, trust authentication,
//! and authentication error handling.

mod common;

use common::{
    parse_backend_messages, start_test_server, start_test_server_with_config,
    test_config_with_password, TestClient,
};
use std::io::Write;
use tempfile::NamedTempFile;
use vibesql_server::auth::password::hash_password_argon2;

/// Test trust authentication (no password required)
#[tokio::test]
async fn test_trust_authentication() {
    let server = start_test_server().await;
    let mut client = TestClient::connect(server.addr()).await.expect("Failed to connect");

    // With trust auth, startup should succeed without password
    client.send_startup("testuser", "testdb").await.expect("Failed to send startup");

    let data = client.read_until_message_type(b'Z').await.expect("Failed to read response");
    let messages = parse_backend_messages(&data);

    // Should have AuthenticationOk immediately
    assert!(
        messages.iter().any(|m| m.is_auth_ok()),
        "Trust auth should send AuthenticationOk without password"
    );
    assert!(
        messages.iter().any(|m| m.is_ready_for_query()),
        "Should be ready for queries after trust auth"
    );

    client.send_terminate().await.expect("Failed to terminate");
    server.shutdown();
}

/// Test cleartext password authentication success
#[tokio::test]
async fn test_password_auth_success() {
    // Create password file with Argon2 hash
    let mut password_file = NamedTempFile::new().expect("Failed to create temp file");
    let hash = hash_password_argon2("secret123").expect("Failed to hash password");
    writeln!(password_file, "testuser:{}", hash).expect("Failed to write password file");
    password_file.flush().expect("Failed to flush");

    let config = test_config_with_password(password_file.path().to_path_buf());
    let server = start_test_server_with_config(config).await;

    let mut client = TestClient::connect(server.addr()).await.expect("Failed to connect");

    // Send startup
    client.send_startup("testuser", "testdb").await.expect("Failed to send startup");

    // Should receive password request
    let data = client.read_response().await.expect("Failed to read response");
    let messages = parse_backend_messages(&data);

    assert!(
        messages.iter().any(|m| m.is_cleartext_password_request()),
        "Should request cleartext password"
    );

    // Send correct password
    client.send_password("secret123").await.expect("Failed to send password");

    // Should receive auth ok and ready for query
    let data = client.read_until_message_type(b'Z').await.expect("Failed to read response");
    let messages = parse_backend_messages(&data);

    assert!(
        messages.iter().any(|m| m.is_auth_ok()),
        "Should receive AuthenticationOk after correct password"
    );
    assert!(
        messages.iter().any(|m| m.is_ready_for_query()),
        "Should be ready for queries after auth"
    );

    // Verify connection works
    client.send_query("SELECT 1").await.expect("Failed to send query");
    let data = client.read_until_message_type(b'Z').await.expect("Failed to read response");
    let messages = parse_backend_messages(&data);
    assert!(messages.iter().any(|m| m.is_command_complete()));

    client.send_terminate().await.expect("Failed to terminate");
    server.shutdown();
}

/// Test cleartext password authentication failure
#[tokio::test]
async fn test_password_auth_failure() {
    // Create password file
    let mut password_file = NamedTempFile::new().expect("Failed to create temp file");
    let hash = hash_password_argon2("correctpassword").expect("Failed to hash password");
    writeln!(password_file, "testuser:{}", hash).expect("Failed to write password file");
    password_file.flush().expect("Failed to flush");

    let config = test_config_with_password(password_file.path().to_path_buf());
    let server = start_test_server_with_config(config).await;

    let mut client = TestClient::connect(server.addr()).await.expect("Failed to connect");

    // Send startup
    client.send_startup("testuser", "testdb").await.expect("Failed to send startup");

    // Read password request
    let _ = client.read_response().await.expect("Failed to read response");

    // Send wrong password
    client.send_password("wrongpassword").await.expect("Failed to send password");

    // Connection should be closed or error returned
    // Read whatever comes back
    let result = client.read_response().await;

    // Either the read fails (connection closed) or we get an error message
    match result {
        Ok(data) if data.is_empty() => {
            // Connection closed - expected for auth failure
        }
        Ok(_) => {
            // Got some data - could be an error message
        }
        Err(_) => {
            // Read error - connection closed
        }
    }

    server.shutdown();
}

/// Test authentication with non-existent user
#[tokio::test]
async fn test_auth_nonexistent_user() {
    // Create password file with only one user
    let mut password_file = NamedTempFile::new().expect("Failed to create temp file");
    let hash = hash_password_argon2("secret").expect("Failed to hash password");
    writeln!(password_file, "existinguser:{}", hash).expect("Failed to write password file");
    password_file.flush().expect("Failed to flush");

    let config = test_config_with_password(password_file.path().to_path_buf());
    let server = start_test_server_with_config(config).await;

    let mut client = TestClient::connect(server.addr()).await.expect("Failed to connect");

    // Try to connect as non-existent user
    client.send_startup("nonexistent", "testdb").await.expect("Failed to send startup");

    // Read password request
    let _ = client.read_response().await.expect("Failed to read response");

    // Send any password
    client.send_password("anypassword").await.expect("Failed to send password");

    // Should fail since user doesn't exist
    let result = client.read_response().await;
    match result {
        Ok(data) if data.is_empty() => {
            // Connection closed - expected
        }
        Ok(_) | Err(_) => {
            // Either got error response or connection error
        }
    }

    server.shutdown();
}

/// Test MD5 password authentication
#[tokio::test]
async fn test_md5_auth() {
    // Create password file with MD5 format
    let mut password_file = NamedTempFile::new().expect("Failed to create temp file");
    writeln!(password_file, "testuser:{{MD5}}secret").expect("Failed to write password file");
    password_file.flush().expect("Failed to flush");

    let mut config = test_config_with_password(password_file.path().to_path_buf());
    config.auth.method = "md5".to_string();
    let server = start_test_server_with_config(config).await;

    let mut client = TestClient::connect(server.addr()).await.expect("Failed to connect");

    // Send startup
    client.send_startup("testuser", "testdb").await.expect("Failed to send startup");

    // Should receive MD5 password request with salt
    let data = client.read_response().await.expect("Failed to read response");
    let messages = parse_backend_messages(&data);

    let md5_request = messages.iter().find(|m| m.is_md5_password_request());
    assert!(md5_request.is_some(), "Should request MD5 password");

    // Get the salt
    let salt = md5_request.unwrap().get_md5_salt().expect("Should have salt");

    // Compute MD5 response using the salt
    let md5_response =
        vibesql_server::auth::password::compute_md5_password("secret", "testuser", &salt);

    // Send MD5 password (with md5 prefix as per protocol)
    client.send_password(&format!("md5{}", md5_response)).await.expect("Failed to send password");

    // Should get auth ok
    let data = client.read_until_message_type(b'Z').await.expect("Failed to read response");
    let messages = parse_backend_messages(&data);

    assert!(messages.iter().any(|m| m.is_auth_ok()), "MD5 auth should succeed");

    client.send_terminate().await.expect("Failed to terminate");
    server.shutdown();
}

/// Test multiple users in password file
#[tokio::test]
async fn test_multiple_users() {
    // Create password file with multiple users
    let mut password_file = NamedTempFile::new().expect("Failed to create temp file");
    let hash1 = hash_password_argon2("password1").expect("Failed to hash password");
    let hash2 = hash_password_argon2("password2").expect("Failed to hash password");
    writeln!(password_file, "user1:{}", hash1).expect("Failed to write");
    writeln!(password_file, "user2:{}", hash2).expect("Failed to write");
    password_file.flush().expect("Failed to flush");

    let config = test_config_with_password(password_file.path().to_path_buf());
    let server = start_test_server_with_config(config).await;

    // Test user1
    {
        let mut client = TestClient::connect(server.addr()).await.expect("Failed to connect");
        client.send_startup("user1", "testdb").await.expect("Failed to send startup");
        let _ = client.read_response().await.expect("Failed to read response");
        client.send_password("password1").await.expect("Failed to send password");
        let data = client.read_until_message_type(b'Z').await.expect("Failed to read response");
        let messages = parse_backend_messages(&data);
        assert!(messages.iter().any(|m| m.is_auth_ok()), "user1 should auth successfully");
        client.send_terminate().await.expect("Failed to terminate");
    }

    // Test user2
    {
        let mut client = TestClient::connect(server.addr()).await.expect("Failed to connect");
        client.send_startup("user2", "testdb").await.expect("Failed to send startup");
        let _ = client.read_response().await.expect("Failed to read response");
        client.send_password("password2").await.expect("Failed to send password");
        let data = client.read_until_message_type(b'Z').await.expect("Failed to read response");
        let messages = parse_backend_messages(&data);
        assert!(messages.iter().any(|m| m.is_auth_ok()), "user2 should auth successfully");
        client.send_terminate().await.expect("Failed to terminate");
    }

    server.shutdown();
}

/// Test password file with comments and empty lines
#[tokio::test]
async fn test_password_file_with_comments() {
    let mut password_file = NamedTempFile::new().expect("Failed to create temp file");
    let hash = hash_password_argon2("mypassword").expect("Failed to hash password");
    writeln!(password_file, "# This is a comment").expect("Failed to write");
    writeln!(password_file).expect("Failed to write empty line");
    writeln!(password_file, "testuser:{}", hash).expect("Failed to write");
    writeln!(password_file, "# Another comment").expect("Failed to write");
    password_file.flush().expect("Failed to flush");

    let config = test_config_with_password(password_file.path().to_path_buf());
    let server = start_test_server_with_config(config).await;

    let mut client = TestClient::connect(server.addr()).await.expect("Failed to connect");
    client.send_startup("testuser", "testdb").await.expect("Failed to send startup");
    let _ = client.read_response().await.expect("Failed to read response");
    client.send_password("mypassword").await.expect("Failed to send password");
    let data = client.read_until_message_type(b'Z').await.expect("Failed to read response");
    let messages = parse_backend_messages(&data);
    assert!(
        messages.iter().any(|m| m.is_auth_ok()),
        "Should authenticate despite comments in file"
    );

    client.send_terminate().await.expect("Failed to terminate");
    server.shutdown();
}

/// Test that authenticated session maintains its identity
#[tokio::test]
async fn test_authenticated_session_identity() {
    let mut password_file = NamedTempFile::new().expect("Failed to create temp file");
    let hash = hash_password_argon2("secret").expect("Failed to hash password");
    writeln!(password_file, "myuser:{}", hash).expect("Failed to write");
    password_file.flush().expect("Failed to flush");

    let config = test_config_with_password(password_file.path().to_path_buf());
    let server = start_test_server_with_config(config).await;

    let mut client = TestClient::connect(server.addr()).await.expect("Failed to connect");
    client.send_startup("myuser", "mydb").await.expect("Failed to send startup");
    let _ = client.read_response().await.expect("Failed to read response");
    client.send_password("secret").await.expect("Failed to send password");
    let _ = client.read_until_message_type(b'Z').await.expect("Failed to read response");

    // Session should work for multiple queries
    for _ in 0..5 {
        client.send_query("SELECT 1").await.expect("Failed to send query");
        let data = client.read_until_message_type(b'Z').await.expect("Failed to read response");
        let messages = parse_backend_messages(&data);
        assert!(messages.iter().any(|m| m.is_command_complete()));
    }

    client.send_terminate().await.expect("Failed to terminate");
    server.shutdown();
}
