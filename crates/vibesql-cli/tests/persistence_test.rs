// Integration tests for database file persistence (snapshot / SQL-dump path).
//
// These tests assert the snapshot-on-save behavior: the single `.db` file is a
// SQL text dump whose contents can be inspected directly. WAL is on by default
// (issue #5698), which would instead write a binary checkpoint plus `.wal` /
// `-checkpoints/` sibling files. To keep exercising the snapshot path
// deterministically, every subprocess here runs with a temp `$HOME/.vibesqlrc`
// that sets `wal = false`. The WAL crash-recovery path has its own coverage in
// `wal_recovery_test.rs`.

use std::{fs, path::Path, process::Command};

use tempfile::TempDir;

/// Get the path to the vibesql binary.
/// Uses CARGO_BIN_EXE_vibesql which is set by cargo during test compilation,
/// pointing to the compiled binary in the target directory.
fn vibesql_binary() -> &'static str {
    env!("CARGO_BIN_EXE_vibesql")
}

/// Create a temp `$HOME` containing a `.vibesqlrc` that disables WAL, so these
/// snapshot-path tests are unaffected by the WAL-on-by-default global default
/// and by any sibling files left in `/tmp` from prior runs.
fn snapshot_home() -> TempDir {
    let home = tempfile::tempdir().expect("create temp home");
    fs::write(home.path().join(".vibesqlrc"), "[database]\nwal = false\n")
        .expect("write .vibesqlrc");
    home
}

/// Run `vibesql -c <sql>` against `db` in snapshot (wal = false) mode.
fn run_snapshot(home: &TempDir, db: &str, sql: &str) -> std::process::Output {
    Command::new(vibesql_binary())
        .args(["--database", db, "-c", sql])
        .env("HOME", home.path())
        .output()
        .expect("Failed to execute command")
}

/// Remove a database file and any WAL sibling files that a prior (WAL-enabled)
/// run may have left behind, so the snapshot path starts from a clean slate.
fn clean_db(db: &str) {
    let _ = fs::remove_file(db);
    let p = Path::new(db);
    let _ = fs::remove_file(p.with_extension("wal"));
    if let (Some(parent), Some(stem)) = (p.parent(), p.file_stem()) {
        let _ = fs::remove_dir_all(parent.join(format!("{}-checkpoints", stem.to_string_lossy())));
    }
}

#[test]
fn test_command_mode_persistence() {
    let home = snapshot_home();
    let test_db = "/tmp/test_vibesql_cmd_mode.db";

    // Clean up any existing test file (and WAL siblings from earlier runs)
    clean_db(test_db);

    // Create a table
    let output = run_snapshot(&home, test_db, "CREATE TABLE test_users (id INTEGER, name TEXT)");
    assert!(output.status.success(), "CREATE TABLE should succeed");

    // Insert data
    let output = run_snapshot(&home, test_db, "INSERT INTO test_users VALUES (1, 'Alice')");
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
    let output = run_snapshot(&home, test_db, "SELECT * FROM test_users");
    assert!(output.status.success(), "SELECT should succeed");
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("Alice"), "Query should return inserted data");

    // Clean up
    clean_db(test_db);
}

#[test]
fn test_multiple_sessions_persistence() {
    let home = snapshot_home();
    let test_db = "/tmp/test_vibesql_multi_session.db";

    // Clean up any existing test file (and WAL siblings from earlier runs)
    clean_db(test_db);

    // Session 1: Create table
    let output =
        run_snapshot(&home, test_db, "CREATE TABLE products (id INTEGER, name TEXT, price REAL)");
    assert!(output.status.success());

    // Session 2: Insert first row
    let output = run_snapshot(&home, test_db, "INSERT INTO products VALUES (1, 'Widget', 9.99)");
    assert!(output.status.success());

    // Session 3: Insert second row
    let output = run_snapshot(&home, test_db, "INSERT INTO products VALUES (2, 'Gadget', 19.99)");
    assert!(output.status.success());

    // Session 4: Query all data
    let output = run_snapshot(&home, test_db, "SELECT * FROM products");
    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("Widget"), "Should contain first row");
    assert!(stdout.contains("Gadget"), "Should contain second row");

    // Clean up
    clean_db(test_db);
}

#[test]
fn test_binary_file_error_message() {
    // Create a fake binary database file (simulating SQLite with non-UTF8 bytes)
    let home = snapshot_home();
    let test_db = "/tmp/test_vibesql_binary.db";
    clean_db(test_db);
    let mut binary_data = b"SQLite format 3\0".to_vec();
    // Add some non-UTF8 bytes
    binary_data.extend_from_slice(&[0xFF, 0xFE, 0xFD, 0xFC, 0x00, 0x01, 0x02]);
    fs::write(test_db, binary_data).expect("Failed to create test file");

    // Try to open it with vibesql
    let output = run_snapshot(&home, test_db, "SELECT 1");

    assert!(!output.status.success(), "Should fail for binary file");

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("binary SQLite database") || stderr.contains("SQL dump format"),
        "Error message should mention binary format and SQL dump format. Got: {}",
        stderr
    );

    // Clean up
    clean_db(test_db);
}

#[test]
fn test_new_database_file_creation() {
    let home = snapshot_home();
    let test_db = "/tmp/test_vibesql_new_file.db";

    // Clean up any existing test file (and WAL siblings from earlier runs)
    clean_db(test_db);

    // Create database with a non-existent file path
    let output = run_snapshot(&home, test_db, "CREATE TABLE new_table (id INTEGER)");
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
    clean_db(test_db);
}

#[test]
fn test_untyped_column_persistence() {
    // Regression test for issue #4324: Untyped columns persist as BLOB, breaking reloads
    // When a table with untyped columns is persisted and reloaded, it should still
    // accept any value type (SQLite type affinity behavior).
    let home = snapshot_home();
    let test_db = "/tmp/test_vibesql_untyped_column.db";

    // Clean up any existing test file (and WAL siblings from earlier runs)
    clean_db(test_db);

    // Session 1: Create table with untyped column
    let output = run_snapshot(&home, test_db, "CREATE TABLE t(a)");
    assert!(output.status.success(), "CREATE TABLE should succeed");

    // Session 2: Insert integer value (this was failing before the fix)
    let output = run_snapshot(&home, test_db, "INSERT INTO t VALUES(1)");
    assert!(
        output.status.success(),
        "INSERT integer should succeed after reload. stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    // Session 3: Insert string value
    let output = run_snapshot(&home, test_db, "INSERT INTO t VALUES('hello')");
    assert!(output.status.success(), "INSERT string should succeed after reload");

    // Session 4: Insert float value
    let output = run_snapshot(&home, test_db, "INSERT INTO t VALUES(3.14)");
    assert!(output.status.success(), "INSERT float should succeed after reload");

    // Session 5: Query all data to verify persistence
    let output = run_snapshot(&home, test_db, "SELECT * FROM t");
    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("1"), "Should contain integer value");
    assert!(stdout.contains("hello"), "Should contain string value");
    assert!(stdout.contains("3.14"), "Should contain float value");

    // Clean up
    clean_db(test_db);
}
