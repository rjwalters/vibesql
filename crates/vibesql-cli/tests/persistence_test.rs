// Integration tests for database file persistence

use std::fs;
use std::process::Command;

/// Get the path to the vibesql binary.
/// Uses CARGO_BIN_EXE_vibesql which is set by cargo during test compilation,
/// pointing to the compiled binary in the target directory.
fn vibesql_binary() -> &'static str {
    env!("CARGO_BIN_EXE_vibesql")
}

#[test]
fn test_command_mode_persistence() {
    let test_db = "/tmp/test_vibesql_cmd_mode.db";

    // Clean up any existing test file
    let _ = fs::remove_file(test_db);

    // Create a table
    let output = Command::new(vibesql_binary())
        .args(["--database", test_db, "-c", "CREATE TABLE test_users (id INTEGER, name TEXT)"])
        .output()
        .expect("Failed to execute command");

    assert!(output.status.success(), "CREATE TABLE should succeed");

    // Insert data
    let output = Command::new(vibesql_binary())
        .args(["--database", test_db, "-c", "INSERT INTO test_users VALUES (1, 'Alice')"])
        .output()
        .expect("Failed to execute command");

    assert!(output.status.success(), "INSERT should succeed");

    // Verify the file was created
    assert!(std::path::Path::new(test_db).exists(), "Database file should exist");

    // Verify the file contains the expected SQL (note: identifiers are uppercased)
    let content = fs::read_to_string(test_db).expect("Should be able to read database file");
    assert!(
        content.to_uppercase().contains("CREATE TABLE TEST_USERS"),
        "Database should contain CREATE TABLE statement"
    );
    assert!(content.contains("Alice"), "Database should contain inserted data");

    // Query the data in a new session
    let output = Command::new(vibesql_binary())
        .args(["--database", test_db, "-c", "SELECT * FROM test_users"])
        .output()
        .expect("Failed to execute command");

    assert!(output.status.success(), "SELECT should succeed");
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("Alice"), "Query should return inserted data");

    // Clean up
    let _ = fs::remove_file(test_db);
}

#[test]
fn test_multiple_sessions_persistence() {
    let test_db = "/tmp/test_vibesql_multi_session.db";

    // Clean up any existing test file
    let _ = fs::remove_file(test_db);

    // Session 1: Create table
    let output = Command::new(vibesql_binary())
        .args([
            "--database",
            test_db,
            "-c",
            "CREATE TABLE products (id INTEGER, name TEXT, price REAL)",
        ])
        .output()
        .expect("Failed to execute command");

    assert!(output.status.success());

    // Session 2: Insert first row
    let output = Command::new(vibesql_binary())
        .args(["--database", test_db, "-c", "INSERT INTO products VALUES (1, 'Widget', 9.99)"])
        .output()
        .expect("Failed to execute command");

    assert!(output.status.success());

    // Session 3: Insert second row
    let output = Command::new(vibesql_binary())
        .args(["--database", test_db, "-c", "INSERT INTO products VALUES (2, 'Gadget', 19.99)"])
        .output()
        .expect("Failed to execute command");

    assert!(output.status.success());

    // Session 4: Query all data
    let output = Command::new(vibesql_binary())
        .args(["--database", test_db, "-c", "SELECT * FROM products"])
        .output()
        .expect("Failed to execute command");

    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("Widget"), "Should contain first row");
    assert!(stdout.contains("Gadget"), "Should contain second row");

    // Clean up
    let _ = fs::remove_file(test_db);
}

#[test]
fn test_binary_file_error_message() {
    // Create a fake binary database file (simulating SQLite with non-UTF8 bytes)
    let test_db = "/tmp/test_vibesql_binary.db";
    let mut binary_data = b"SQLite format 3\0".to_vec();
    // Add some non-UTF8 bytes
    binary_data.extend_from_slice(&[0xFF, 0xFE, 0xFD, 0xFC, 0x00, 0x01, 0x02]);
    fs::write(test_db, binary_data).expect("Failed to create test file");

    // Try to open it with vibesql
    let output = Command::new(vibesql_binary())
        .args(["--database", test_db, "-c", "SELECT 1"])
        .output()
        .expect("Failed to execute command");

    assert!(!output.status.success(), "Should fail for binary file");

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("binary SQLite database") || stderr.contains("SQL dump format"),
        "Error message should mention binary format and SQL dump format. Got: {}",
        stderr
    );

    // Clean up
    let _ = fs::remove_file(test_db);
}

#[test]
fn test_new_database_file_creation() {
    let test_db = "/tmp/test_vibesql_new_file.db";

    // Clean up any existing test file
    let _ = fs::remove_file(test_db);

    // Create database with a non-existent file path
    let output = Command::new(vibesql_binary())
        .args(["--database", test_db, "-c", "CREATE TABLE new_table (id INTEGER)"])
        .output()
        .expect("Failed to execute command");

    assert!(output.status.success(), "Should succeed creating new database file");

    // Verify the file was created
    assert!(std::path::Path::new(test_db).exists(), "Database file should be created");

    // Verify it contains the table (note: identifiers are uppercased)
    let content = fs::read_to_string(test_db).expect("Should be able to read database file");
    assert!(
        content.to_uppercase().contains("CREATE TABLE NEW_TABLE"),
        "Database should contain the table"
    );

    // Clean up
    let _ = fs::remove_file(test_db);
}
