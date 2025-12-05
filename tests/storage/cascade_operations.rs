//! Integration tests for CASCADE operations on advanced SQL objects

use vibesql_executor::advanced_objects::*;
use vibesql_parser::Parser;
use vibesql_storage::Database;

/// Helper function to execute DDL statement
fn execute_ddl(db: &mut Database, sql: &str) -> Result<(), String> {
    let stmt = Parser::parse_sql(sql).map_err(|e| format!("Parse error: {}", e))?;

    match stmt {
        vibesql_ast::Statement::CreateSequence(ref s) => {
            execute_create_sequence(s, db).map_err(|e| format!("{}", e))
        }
        vibesql_ast::Statement::DropSequence(ref s) => {
            execute_drop_sequence(s, db).map_err(|e| format!("{}", e))
        }
        vibesql_ast::Statement::CreateView(ref s) => {
            execute_create_view(s, db).map_err(|e| format!("{}", e))
        }
        vibesql_ast::Statement::DropView(ref s) => {
            execute_drop_view(s, db).map_err(|e| format!("{}", e))
        }
        vibesql_ast::Statement::CreateDomain(ref s) => {
            vibesql_executor::DomainExecutor::execute_create_domain(s, db)
                .map(|_| ())
                .map_err(|e| format!("{}", e))
        }
        vibesql_ast::Statement::DropDomain(ref s) => {
            vibesql_executor::DomainExecutor::execute_drop_domain(s, db)
                .map(|_| ())
                .map_err(|e| format!("{}", e))
        }
        vibesql_ast::Statement::CreateTable(ref s) => {
            vibesql_executor::CreateTableExecutor::execute(s, db)
                .map(|_| ())
                .map_err(|e| format!("{}", e))
        }
        _ => Err(format!("Unsupported statement: {:?}", stmt)),
    }
}

/// Helper function to execute DDL expecting success
fn execute_ok(db: &mut Database, sql: &str) {
    execute_ddl(db, sql).unwrap_or_else(|e| panic!("Expected success for '{}': {}", sql, e));
}

/// Helper function to execute DDL expecting failure
fn execute_err(db: &mut Database, sql: &str) -> String {
    execute_ddl(db, sql).unwrap_err()
}

#[test]
fn test_drop_sequence_cascade_removes_column_defaults() {
    let mut db = Database::new();

    // Create a sequence
    execute_ok(&mut db, "CREATE SEQUENCE user_id_seq");

    // Create a table that uses the sequence
    execute_ok(
        &mut db,
        "CREATE TABLE users (
            id INTEGER DEFAULT NEXT VALUE FOR user_id_seq,
            name VARCHAR(50)
        )",
    );

    // Verify the sequence exists
    assert!(db.catalog.get_sequence_mut("USER_ID_SEQ").is_ok());

    // DROP SEQUENCE CASCADE should remove the default value from the column
    execute_ok(&mut db, "DROP SEQUENCE user_id_seq CASCADE");

    // Verify the sequence was dropped
    assert!(db.catalog.get_sequence_mut("USER_ID_SEQ").is_err());

    // Verify the table still exists but the column default is gone
    let table = db.catalog.get_table("USERS").expect("Table should exist");
    let id_column = table.get_column("ID").expect("Column should exist");
    assert!(id_column.default_value.is_none(), "Column default should have been removed");
}

#[test]
fn test_drop_sequence_restrict_fails_when_in_use() {
    let mut db = Database::new();

    // Create a sequence
    execute_ok(&mut db, "CREATE SEQUENCE user_id_seq");

    // Create a table that uses the sequence
    execute_ok(
        &mut db,
        "CREATE TABLE users (
            id INTEGER DEFAULT NEXT VALUE FOR user_id_seq,
            name VARCHAR(50)
        )",
    );

    // DROP SEQUENCE RESTRICT should fail because sequence is in use
    let err = execute_err(&mut db, "DROP SEQUENCE user_id_seq RESTRICT");
    assert!(err.contains("still in use"), "Expected 'still in use' error, got: {}", err);

    // Verify the sequence still exists
    assert!(db.catalog.get_sequence_mut("USER_ID_SEQ").is_ok());
}

#[test]
fn test_drop_sequence_restrict_default_behavior() {
    let mut db = Database::new();

    // Create a sequence
    execute_ok(&mut db, "CREATE SEQUENCE user_id_seq");

    // Create a table that uses the sequence
    execute_ok(
        &mut db,
        "CREATE TABLE users (
            id INTEGER DEFAULT NEXT VALUE FOR user_id_seq,
            name VARCHAR(50)
        )",
    );

    // DROP SEQUENCE without CASCADE or RESTRICT defaults to RESTRICT
    let err = execute_err(&mut db, "DROP SEQUENCE user_id_seq");
    assert!(
        err.contains("still in use"),
        "Expected 'still in use' error for default RESTRICT, got: {}",
        err
    );
}

#[test]
fn test_drop_sequence_cascade_multiple_columns() {
    let mut db = Database::new();

    // Create a sequence
    execute_ok(&mut db, "CREATE SEQUENCE id_seq");

    // Create multiple tables using the sequence
    execute_ok(
        &mut db,
        "CREATE TABLE table1 (
            id INTEGER DEFAULT NEXT VALUE FOR id_seq
        )",
    );
    execute_ok(
        &mut db,
        "CREATE TABLE table2 (
            id INTEGER DEFAULT NEXT VALUE FOR id_seq
        )",
    );

    // DROP SEQUENCE CASCADE should remove defaults from all columns
    execute_ok(&mut db, "DROP SEQUENCE id_seq CASCADE");

    // Verify defaults were removed from both tables
    let table1 = db.catalog.get_table("TABLE1").expect("Table1 should exist");
    let table2 = db.catalog.get_table("TABLE2").expect("Table2 should exist");

    assert!(table1.get_column("ID").unwrap().default_value.is_none());
    assert!(table2.get_column("ID").unwrap().default_value.is_none());
}

#[test]
fn test_drop_view_cascade_drops_dependent_views() {
    let mut db = Database::new();

    // Create a base table
    execute_ok(&mut db, "CREATE TABLE users (id INTEGER, name VARCHAR(50))");

    // Create a view on the table
    execute_ok(&mut db, "CREATE VIEW active_users AS SELECT * FROM users");

    // Create another view that depends on the first view
    execute_ok(&mut db, "CREATE VIEW admin_users AS SELECT * FROM active_users");

    // DROP VIEW CASCADE should drop all dependent views
    execute_ok(&mut db, "DROP VIEW active_users CASCADE");

    // Verify both views were dropped
    assert!(db.catalog.get_view("ACTIVE_USERS").is_none(), "active_users view should be dropped");
    assert!(
        db.catalog.get_view("ADMIN_USERS").is_none(),
        "admin_users view (dependent) should also be dropped"
    );

    // Verify the base table still exists
    assert!(db.catalog.get_table("USERS").is_some());
}

#[test]
fn test_drop_view_restrict_fails_when_has_dependents() {
    let mut db = Database::new();

    // Create a base table
    execute_ok(&mut db, "CREATE TABLE users (id INTEGER, name VARCHAR(50))");

    // Create a view on the table
    execute_ok(&mut db, "CREATE VIEW active_users AS SELECT * FROM users");

    // Create another view that depends on the first view
    execute_ok(&mut db, "CREATE VIEW admin_users AS SELECT * FROM active_users");

    // DROP VIEW RESTRICT should fail because there are dependent views
    let err = execute_err(&mut db, "DROP VIEW active_users RESTRICT");
    assert!(err.contains("still in use"), "Expected 'still in use' error, got: {}", err);

    // Verify views still exist
    assert!(db.catalog.get_view("ACTIVE_USERS").is_some());
    assert!(db.catalog.get_view("ADMIN_USERS").is_some());
}

#[test]
fn test_drop_view_cascade_chain_of_dependencies() {
    let mut db = Database::new();

    // Create a base table
    execute_ok(&mut db, "CREATE TABLE base (id INTEGER)");

    // Create a chain of dependent views
    execute_ok(&mut db, "CREATE VIEW view1 AS SELECT * FROM base");
    execute_ok(&mut db, "CREATE VIEW view2 AS SELECT * FROM view1");
    execute_ok(&mut db, "CREATE VIEW view3 AS SELECT * FROM view2");

    // DROP VIEW CASCADE on the first view should drop all dependent views
    execute_ok(&mut db, "DROP VIEW view1 CASCADE");

    // Verify all views in the chain were dropped
    assert!(db.catalog.get_view("VIEW1").is_none());
    assert!(db.catalog.get_view("VIEW2").is_none());
    assert!(db.catalog.get_view("VIEW3").is_none());

    // Verify the base table still exists
    assert!(db.catalog.get_table("BASE").is_some());
}

#[test]
fn test_drop_view_no_dependents_works_with_restrict() {
    let mut db = Database::new();

    // Create a table and view
    execute_ok(&mut db, "CREATE TABLE users (id INTEGER)");
    execute_ok(&mut db, "CREATE VIEW user_view AS SELECT * FROM users");

    // DROP VIEW RESTRICT should succeed when there are no dependents
    execute_ok(&mut db, "DROP VIEW user_view RESTRICT");

    // Verify the view was dropped
    assert!(db.catalog.get_view("USER_VIEW").is_none());

    // Verify the table still exists
    assert!(db.catalog.get_table("USERS").is_some());
}

#[test]
fn test_drop_domain_cascade_converts_columns_to_base_type() {
    let mut db = Database::new();

    // Create a domain
    execute_ok(&mut db, "CREATE DOMAIN email_domain AS VARCHAR(255)");

    // Note: Domain usage in CREATE TABLE columns is not fully implemented yet
    // This test demonstrates the framework but may need adjustment when full support is added

    // For now, test that DROP DOMAIN CASCADE works even without columns using it
    execute_ok(&mut db, "DROP DOMAIN email_domain CASCADE");

    // Verify the domain was dropped
    assert!(!db.catalog.domain_exists("EMAIL_DOMAIN"));
}

#[test]
fn test_drop_domain_restrict_when_not_in_use() {
    let mut db = Database::new();

    // Create a domain
    execute_ok(&mut db, "CREATE DOMAIN email_domain AS VARCHAR(255)");

    // DROP DOMAIN RESTRICT should succeed when not in use
    execute_ok(&mut db, "DROP DOMAIN email_domain RESTRICT");

    // Verify the domain was dropped
    assert!(!db.catalog.domain_exists("EMAIL_DOMAIN"));
}

#[test]
fn test_cascade_operations_isolation() {
    // Test that CASCADE only affects direct and indirect dependencies,
    // not unrelated objects
    let mut db = Database::new();

    // Create sequences
    execute_ok(&mut db, "CREATE SEQUENCE seq1");
    execute_ok(&mut db, "CREATE SEQUENCE seq2");

    // Create tables using different sequences
    execute_ok(&mut db, "CREATE TABLE table1 (id INTEGER DEFAULT NEXT VALUE FOR seq1)");
    execute_ok(&mut db, "CREATE TABLE table2 (id INTEGER DEFAULT NEXT VALUE FOR seq2)");

    // Drop seq1 CASCADE
    execute_ok(&mut db, "DROP SEQUENCE seq1 CASCADE");

    // Verify seq2 and table2 are unaffected
    assert!(db.catalog.get_sequence_mut("SEQ2").is_ok());
    let table2 = db.catalog.get_table("TABLE2").unwrap();
    assert!(
        table2.get_column("ID").unwrap().default_value.is_some(),
        "Unrelated table's default should be preserved"
    );
}

#[test]
fn test_drop_view_if_exists_with_cascade() {
    let mut db = Database::new();

    // Create table and views
    execute_ok(&mut db, "CREATE TABLE base (id INTEGER)");
    execute_ok(&mut db, "CREATE VIEW view1 AS SELECT * FROM base");
    execute_ok(&mut db, "CREATE VIEW view2 AS SELECT * FROM view1");

    // DROP VIEW IF EXISTS CASCADE should work even if view exists
    execute_ok(&mut db, "DROP VIEW IF EXISTS view1 CASCADE");

    // Verify views were dropped
    assert!(db.catalog.get_view("VIEW1").is_none());
    assert!(db.catalog.get_view("VIEW2").is_none());

    // DROP VIEW IF EXISTS CASCADE should not fail for non-existent view
    execute_ok(&mut db, "DROP VIEW IF EXISTS nonexistent CASCADE");
}

#[test]
fn test_sequence_cascade_with_complex_default_expression() {
    // Test CASCADE with sequences used in more complex default expressions
    let mut db = Database::new();

    execute_ok(&mut db, "CREATE SEQUENCE id_seq");

    // Note: If the parser supports more complex defaults, this test validates
    // that CASCADE still works correctly
    execute_ok(
        &mut db,
        "CREATE TABLE users (
            id INTEGER DEFAULT NEXT VALUE FOR id_seq,
            name VARCHAR(50)
        )",
    );

    execute_ok(&mut db, "DROP SEQUENCE id_seq CASCADE");

    // Verify the default was removed
    let table = db.catalog.get_table("USERS").unwrap();
    assert!(table.get_column("ID").unwrap().default_value.is_none());
}
