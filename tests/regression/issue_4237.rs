//! Test for Issue #4237: INSERT OR REPLACE fails on persisted databases
//!
//! This regression test ensures that INSERT OR REPLACE correctly replaces
//! existing rows when the database is saved to disk (as SQL dump) and reloaded.
//!
//! Root cause: The SQL dump generation (`save_sql_dump`) did not include the
//! PRIMARY KEY constraint in the CREATE TABLE statement. Instead, it only
//! created a UNIQUE INDEX named "PK_<table>". When the database was reloaded,
//! the table had no PRIMARY KEY metadata, so `handle_replace_conflicts()`
//! couldn't detect conflicts and the INSERT OR REPLACE failed with a
//! unique constraint violation.
//!
//! Fix: Modified `save_sql_dump` to:
//! 1. Include PRIMARY KEY constraint in CREATE TABLE statement
//! 2. Skip auto-generated PK indexes (PK_*) to avoid duplicate index creation

use std::fs;
use vibesql_executor::{CreateTableExecutor, InsertExecutor, SelectExecutor};
use vibesql_parser::Parser;
use vibesql_storage::Database;
use vibesql_types::{SqlValue, StringValue};

/// Helper to execute SQL and return results
fn execute(db: &mut Database, sql: &str) -> Result<Vec<Vec<SqlValue>>, String> {
    let stmt = Parser::parse_sql(sql).map_err(|e| format!("Parse error: {}", e))?;
    match stmt {
        vibesql_ast::Statement::Select(select_stmt) => {
            let executor = SelectExecutor::new(db);
            let rows =
                executor.execute(&select_stmt).map_err(|e| format!("Execution error: {}", e))?;
            Ok(rows.into_iter().map(|r| r.values.into_vec()).collect())
        }
        vibesql_ast::Statement::CreateTable(create_stmt) => {
            CreateTableExecutor::execute(&create_stmt, db)
                .map_err(|e| format!("Create table error: {}", e))?;
            Ok(vec![])
        }
        vibesql_ast::Statement::Insert(insert_stmt) => {
            InsertExecutor::execute(db, &insert_stmt).map_err(|e| format!("Insert error: {}", e))?;
            Ok(vec![])
        }
        _ => Err("Unsupported statement type".to_string()),
    }
}

/// Test that INSERT OR REPLACE works correctly after SQL dump save/reload
#[test]
fn test_insert_or_replace_persists_across_sql_dump() {
    // Create database with PRIMARY KEY table
    let mut db = Database::new();

    execute(&mut db, "CREATE TABLE test_files (file_path VARCHAR(500) PRIMARY KEY, status VARCHAR(20) NOT NULL)").unwrap();

    // First INSERT OR REPLACE
    execute(
        &mut db,
        "INSERT OR REPLACE INTO test_files (file_path, status) VALUES ('test.file', 'PASS')",
    )
    .unwrap();

    // Verify initial insert
    let rows = execute(&mut db, "SELECT file_path, status FROM test_files").unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][1], SqlValue::Varchar(StringValue::from("PASS")));

    // Save database as SQL dump
    let path = "/tmp/test_issue_4237.sql";
    db.save_sql_dump(path).unwrap();

    // Verify the SQL dump contains PRIMARY KEY
    let dump_content = fs::read_to_string(path).unwrap();
    assert!(
        dump_content.contains("PRIMARY KEY"),
        "SQL dump should contain PRIMARY KEY constraint"
    );

    // Reload database from SQL dump (simulates opening existing database file)
    // We need to use the CLI's load_sql_dump functionality here
    // For this test, we'll just re-parse and execute the dump

    // Create a fresh database and execute the dump statements
    let mut db2 = Database::new();

    // Parse and execute each statement from the dump
    for line in dump_content.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() || trimmed.starts_with("--") {
            continue;
        }
        // Only process statements ending with semicolon
        if trimmed.ends_with(';') {
            let sql = trimmed.trim_end_matches(';');
            // Skip empty statements
            if sql.trim().is_empty() {
                continue;
            }
            match execute(&mut db2, sql) {
                Ok(_) => {}
                Err(e) => panic!("Failed to execute dump statement '{}': {}", sql, e),
            }
        }
    }

    // Verify the loaded data
    let rows = execute(&mut db2, "SELECT file_path, status FROM test_files").unwrap();
    assert_eq!(rows.len(), 1, "Should have 1 row after loading dump");

    // THIS IS THE BUG BEING TESTED:
    // INSERT OR REPLACE should work after loading from SQL dump
    execute(
        &mut db2,
        "INSERT OR REPLACE INTO test_files (file_path, status) VALUES ('test.file', 'FAIL')",
    )
    .expect("INSERT OR REPLACE should succeed after loading from SQL dump");

    // Verify the replacement worked
    let rows = execute(&mut db2, "SELECT file_path, status FROM test_files").unwrap();
    assert_eq!(rows.len(), 1, "Should still have 1 row after REPLACE");
    assert_eq!(
        rows[0][1],
        SqlValue::Varchar(StringValue::from("FAIL")),
        "Status should be updated to FAIL"
    );

    // Cleanup
    let _ = fs::remove_file(path);
}

/// Test that SQL dump correctly includes composite PRIMARY KEY
#[test]
fn test_composite_primary_key_in_sql_dump() {
    let mut db = Database::new();

    execute(
        &mut db,
        "CREATE TABLE composite_pk (a INTEGER, b VARCHAR(50), c TEXT, PRIMARY KEY (a, b))",
    )
    .unwrap();

    // Save and check the dump
    let path = "/tmp/test_issue_4237_composite.sql";
    db.save_sql_dump(path).unwrap();

    let dump_content = fs::read_to_string(path).unwrap();
    assert!(
        dump_content.contains("PRIMARY KEY (A, B)"),
        "SQL dump should contain composite PRIMARY KEY: {}",
        dump_content
    );

    // Cleanup
    let _ = fs::remove_file(path);
}

/// Test that PK_ indexes are not duplicated in SQL dump
#[test]
fn test_pk_indexes_not_duplicated_in_dump() {
    let mut db = Database::new();

    execute(&mut db, "CREATE TABLE test (id INTEGER PRIMARY KEY, val TEXT)").unwrap();

    // Save and check the dump
    let path = "/tmp/test_issue_4237_pk_dup.sql";
    db.save_sql_dump(path).unwrap();

    let dump_content = fs::read_to_string(path).unwrap();

    // Should have PRIMARY KEY in CREATE TABLE
    assert!(dump_content.contains("PRIMARY KEY"), "Should have PRIMARY KEY in table definition");

    // Should NOT have separate CREATE INDEX PK_TEST
    assert!(
        !dump_content.contains("CREATE UNIQUE INDEX PK_"),
        "Should not have separate PK_ index: {}",
        dump_content
    );

    // Cleanup
    let _ = fs::remove_file(path);
}
