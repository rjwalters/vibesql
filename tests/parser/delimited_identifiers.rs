//! End-to-end tests for delimited identifier behavior
//!
//! VibeSQL follows SQLite's identifier semantics (issue #5553), which differ
//! from the plain SQL:1999 model: identifiers are ASCII case-folded for
//! *lookup/equality* regardless of quoting — `"MyTable"` collides with
//! `mytable` — while the exact spelling as written is preserved only for
//! *display* (error messages, `sqlite_master`, column headers). This is a
//! deliberate SQLite-compatibility choice, not a `SELECT`-time quirk: it
//! governs `CREATE`-time duplicate detection for tables, columns, and
//! schemas too, so two objects that differ only in case (quoted or not)
//! cannot coexist.
//!
//! These tests verify that:
//! 1. `users` and `"USERS"` refer to the *same* table (case-folded for lookup)
//! 2. Quoting only preserves the original spelling for display, not distinctness
//! 3. Unquoted identifiers are normalized to lowercase for display too
//! 4. Reserved words can be used as identifiers when quoted
//! 5. Special characters (spaces, etc.) work in delimited identifiers

use vibesql_ast::Statement;
use vibesql_executor::{CreateTableExecutor, InsertExecutor, SelectExecutor};
use vibesql_parser::Parser;
use vibesql_storage::{Database, Row};
use vibesql_types::{SqlValue, StringValue};

/// Helper to execute CREATE TABLE statements
fn execute_create_table(db: &mut Database, sql: &str) -> Result<String, String> {
    let stmt = Parser::parse_sql(sql).map_err(|e| format!("Parse error: {:?}", e))?;

    match stmt {
        Statement::CreateTable(create_stmt) => CreateTableExecutor::execute(&create_stmt, db)
            .map_err(|e| format!("Execution error: {:?}", e)),
        other => Err(format!("Expected CREATE TABLE statement, got {:?}", other)),
    }
}

/// Helper to execute SELECT statements
fn execute_select(db: &Database, sql: &str) -> Result<Vec<Row>, String> {
    let stmt = Parser::parse_sql(sql).map_err(|e| format!("Parse error: {:?}", e))?;

    let select_stmt = match stmt {
        Statement::Select(s) => s,
        other => return Err(format!("Expected SELECT statement, got {:?}", other)),
    };

    let executor = SelectExecutor::new(db);
    executor.execute(&select_stmt).map_err(|e| format!("Execution error: {:?}", e))
}

/// Helper to execute INSERT statements
fn execute_insert_sql(db: &mut Database, sql: &str) -> Result<usize, String> {
    let stmt = Parser::parse_sql(sql).map_err(|e| format!("Parse error: {:?}", e))?;

    match stmt {
        Statement::Insert(insert_stmt) => InsertExecutor::execute(db, &insert_stmt)
            .map_err(|e| format!("Execution error: {:?}", e)),
        other => Err(format!("Expected INSERT statement, got {:?}", other)),
    }
}

// ========================================================================
// Core Behavior: Quoted vs Unquoted Table Names
// ========================================================================

#[test]
fn test_quoted_vs_unquoted_table_names() {
    let mut db = Database::new();

    // Create table with unquoted name (normalized to lowercase 'users')
    execute_create_table(&mut db, "CREATE TABLE users (id INT)").unwrap();
    // SQL INSERT with unquoted table name - uses case-insensitive lookup
    execute_insert_sql(&mut db, "INSERT INTO users VALUES (1)").unwrap();

    // A second CREATE TABLE for the SAME name, only quoted and uppercase,
    // must fail: SQLite case-folds table names for lookup regardless of
    // quoting (#5553), so `"USERS"` collides with the `users` created above.
    let err = execute_create_table(&mut db, r#"CREATE TABLE "USERS" (id INT)"#).unwrap_err();
    assert!(err.contains("already exists") || err.contains("TableAlreadyExists"), "{}", err);

    // SQL INSERT with quoted table name - uses the SAME case-folded lookup
    // as the unquoted name, so it targets the one `users` table created above.
    execute_insert_sql(&mut db, r#"INSERT INTO "USERS" VALUES (2)"#).unwrap();

    // Both the unquoted and quoted-uppercase references resolve to the same
    // table, so both inserted rows are visible either way.
    let result1 = execute_select(&db, "SELECT * FROM users").unwrap();
    assert_eq!(result1.len(), 2);

    let result2 = execute_select(&db, r#"SELECT * FROM "USERS""#).unwrap();
    assert_eq!(result2.len(), 2);
}

#[test]
fn test_unquoted_identifier_normalization() {
    let mut db = Database::new();

    // All these variations create the same table (PRODUCTS)
    execute_create_table(&mut db, "CREATE TABLE products (id INT)").unwrap();

    // Different case variations in queries all refer to the same table
    db.insert_row("products", Row::new(vec![SqlValue::Integer(10)])).unwrap();

    let result1 = execute_select(&db, "SELECT * FROM products").unwrap();
    let result2 = execute_select(&db, "SELECT * FROM PRODUCTS").unwrap();
    let result3 = execute_select(&db, "SELECT * FROM PrOdUcTs").unwrap();

    // All queries return the same row
    assert_eq!(result1.len(), 1);
    assert_eq!(result2.len(), 1);
    assert_eq!(result3.len(), 1);
    assert_eq!(result1[0].values[0], SqlValue::Integer(10));
    assert_eq!(result2[0].values[0], SqlValue::Integer(10));
    assert_eq!(result3[0].values[0], SqlValue::Integer(10));
}

#[test]
fn test_quoted_identifier_case_sensitivity() {
    let mut db = Database::new();

    // Only the FIRST quoted spelling creates a table: SQLite case-folds
    // table names for lookup regardless of quoting (#5553), so "PRODUCTS"
    // and "products" both collide with the already-created "Products".
    execute_create_table(&mut db, r#"CREATE TABLE "Products" (id INT)"#).unwrap();
    let err1 = execute_create_table(&mut db, r#"CREATE TABLE "PRODUCTS" (id INT)"#).unwrap_err();
    assert!(err1.contains("TableAlreadyExists"), "{}", err1);
    let err2 = execute_create_table(&mut db, r#"CREATE TABLE "products" (id INT)"#).unwrap_err();
    assert!(err2.contains("TableAlreadyExists"), "{}", err2);

    // Insert through each differently-cased spelling — all resolve to the
    // single "Products" table via the same case-folded lookup.
    execute_insert_sql(&mut db, r#"INSERT INTO "Products" VALUES (1)"#).unwrap();
    execute_insert_sql(&mut db, r#"INSERT INTO "PRODUCTS" VALUES (2)"#).unwrap();
    execute_insert_sql(&mut db, r#"INSERT INTO "products" VALUES (3)"#).unwrap();

    // Every spelling now sees all three inserted rows, since they all name
    // the same underlying table.
    let result1 = execute_select(&db, r#"SELECT * FROM "Products""#).unwrap();
    let result2 = execute_select(&db, r#"SELECT * FROM "PRODUCTS""#).unwrap();
    let result3 = execute_select(&db, r#"SELECT * FROM "products""#).unwrap();

    assert_eq!(result1.len(), 3);
    assert_eq!(result2.len(), 3);
    assert_eq!(result3.len(), 3);
}

// ========================================================================
// Column Names: Quoted vs Unquoted
// ========================================================================

#[test]
fn test_case_sensitive_column_names() {
    let mut db = Database::new();

    // Create table with both quoted and unquoted column names
    // "firstName" preserves case, lastName normalized to LASTNAME
    execute_create_table(
        &mut db,
        r#"CREATE TABLE employees ("firstName" VARCHAR(50), lastName VARCHAR(50))"#,
    )
    .unwrap();

    // Insert a row
    db.insert_row(
        "EMPLOYEES",
        Row::new(vec![
            SqlValue::Varchar(StringValue::from("John")),
            SqlValue::Varchar(StringValue::from("Doe")),
        ]),
    )
    .unwrap();

    // Query with exact case for quoted identifier
    let result1 = execute_select(&db, r#"SELECT "firstName" FROM employees"#).unwrap();
    assert_eq!(result1[0].values[0], SqlValue::Varchar(StringValue::from("John")));

    // Query with any case for unquoted identifier (normalized to LASTNAME)
    let result2 = execute_select(&db, "SELECT lastname FROM employees").unwrap();
    assert_eq!(result2[0].values[0], SqlValue::Varchar(StringValue::from("Doe")));

    let result3 = execute_select(&db, "SELECT LASTNAME FROM employees").unwrap();
    assert_eq!(result3[0].values[0], SqlValue::Varchar(StringValue::from("Doe")));
}

#[test]
fn test_different_case_columns_are_distinct() {
    let mut db = Database::new();

    // SQLite compares column names case-insensitively regardless of quoting
    // (#5553), so three columns differing only in case are a duplicate-column
    // error, not three distinct columns — matching `CREATE TABLE t(a, A)`.
    let err = execute_create_table(
        &mut db,
        r#"CREATE TABLE data ("value" INT, "VALUE" INT, "Value" INT)"#,
    )
    .unwrap_err();
    assert!(err.contains("duplicate column name"), "{}", err);

    // A single quoted column name still works, and is reachable through any
    // case-folded spelling of the same name.
    execute_create_table(&mut db, r#"CREATE TABLE data ("value" INT)"#).unwrap();
    db.insert_row("DATA", Row::new(vec![SqlValue::Integer(1)])).unwrap();

    let result1 = execute_select(&db, r#"SELECT "value" FROM data"#).unwrap();
    let result2 = execute_select(&db, r#"SELECT "VALUE" FROM data"#).unwrap();
    let result3 = execute_select(&db, r#"SELECT "Value" FROM data"#).unwrap();

    assert_eq!(result1[0].values[0], SqlValue::Integer(1));
    assert_eq!(result2[0].values[0], SqlValue::Integer(1));
    assert_eq!(result3[0].values[0], SqlValue::Integer(1));
}

// ========================================================================
// Reserved Words as Identifiers
// ========================================================================

#[test]
fn test_reserved_words_as_table_names() {
    let mut db = Database::new();

    // Cannot use reserved word as unquoted identifier (would fail at parse)
    // But CAN use when quoted
    execute_create_table(&mut db, r#"CREATE TABLE "SELECT" (id INT)"#).unwrap();
    execute_create_table(&mut db, r#"CREATE TABLE "FROM" (id INT)"#).unwrap();
    execute_create_table(&mut db, r#"CREATE TABLE "WHERE" (id INT)"#).unwrap();

    execute_insert_sql(&mut db, r#"INSERT INTO "SELECT" VALUES (42)"#).unwrap();

    let result = execute_select(&db, r#"SELECT * FROM "SELECT""#).unwrap();
    assert_eq!(result[0].values[0], SqlValue::Integer(42));
}

#[test]
fn test_reserved_words_as_column_names() {
    let mut db = Database::new();

    // Use reserved words as column names (must be quoted)
    execute_create_table(
        &mut db,
        r#"CREATE TABLE queries ("SELECT" INT, "FROM" VARCHAR(50), "WHERE" INT)"#,
    )
    .unwrap();

    db.insert_row(
        "QUERIES",
        Row::new(vec![
            SqlValue::Integer(1),
            SqlValue::Varchar(StringValue::from("table1")),
            SqlValue::Integer(100),
        ]),
    )
    .unwrap();

    let result = execute_select(&db, r#"SELECT "SELECT", "FROM", "WHERE" FROM queries"#).unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].values[0], SqlValue::Integer(1));
    assert_eq!(result[0].values[1], SqlValue::Varchar(StringValue::from("table1")));
    assert_eq!(result[0].values[2], SqlValue::Integer(100));
}

// ========================================================================
// Special Characters in Identifiers
// ========================================================================

#[test]
fn test_spaces_in_table_names() {
    let mut db = Database::new();

    // Spaces only allowed in delimited identifiers
    execute_create_table(&mut db, r#"CREATE TABLE "My Table" (id INT)"#).unwrap();

    execute_insert_sql(&mut db, r#"INSERT INTO "My Table" VALUES (99)"#).unwrap();

    let result = execute_select(&db, r#"SELECT * FROM "My Table""#).unwrap();
    assert_eq!(result[0].values[0], SqlValue::Integer(99));
}

#[test]
fn test_spaces_in_column_names() {
    let mut db = Database::new();

    execute_create_table(
        &mut db,
        r#"CREATE TABLE contacts ("First Name" VARCHAR(50), "Last Name" VARCHAR(50))"#,
    )
    .unwrap();

    db.insert_row(
        "CONTACTS",
        Row::new(vec![
            SqlValue::Varchar(StringValue::from("Jane")),
            SqlValue::Varchar(StringValue::from("Smith")),
        ]),
    )
    .unwrap();

    let result = execute_select(&db, r#"SELECT "First Name", "Last Name" FROM contacts"#).unwrap();
    assert_eq!(result[0].values[0], SqlValue::Varchar(StringValue::from("Jane")));
    assert_eq!(result[0].values[1], SqlValue::Varchar(StringValue::from("Smith")));
}

#[test]
fn test_escaped_quotes_in_identifiers() {
    let mut db = Database::new();

    // Double quote inside delimited identifier is escaped with ""
    // "O""Reilly" → O"Reilly
    execute_create_table(
        &mut db,
        r#"CREATE TABLE "O""Reilly Books" (id INT, "Book""Title" VARCHAR(100))"#,
    )
    .unwrap();

    execute_insert_sql(&mut db, r#"INSERT INTO "O""Reilly Books" VALUES (1, 'Learning Rust')"#)
        .unwrap();

    let result = execute_select(&db, r#"SELECT "Book""Title" FROM "O""Reilly Books""#).unwrap();
    assert_eq!(result[0].values[0], SqlValue::Varchar(StringValue::from("Learning Rust")));
}

// ========================================================================
// Mixed Operations: INSERT, UPDATE, DELETE
// ========================================================================

#[test]
fn test_insert_with_delimited_identifiers() {
    let mut db = Database::new();

    execute_create_table(
        &mut db,
        r#"CREATE TABLE "products" ("productId" INT, "productName" VARCHAR(50))"#,
    )
    .unwrap();

    // Parser doesn't support INSERT yet, so we use direct storage API
    db.insert_row(
        "products",
        Row::new(vec![SqlValue::Integer(100), SqlValue::Varchar(StringValue::from("Widget"))]),
    )
    .unwrap();

    let result =
        execute_select(&db, r#"SELECT "productId", "productName" FROM "products""#).unwrap();
    assert_eq!(result[0].values[0], SqlValue::Integer(100));
    assert_eq!(result[0].values[1], SqlValue::Varchar(StringValue::from("Widget")));
}

// ========================================================================
// Error Cases: Non-existent Identifiers
// ========================================================================

#[test]
fn test_error_on_nonexistent_quoted_table() {
    let db = Database::new();

    // "users" doesn't exist (only USERS exists if we created unquoted 'users')
    let result = execute_select(&db, r#"SELECT * FROM "users""#);
    assert!(result.is_err());
}

#[test]
fn test_case_mismatch_quoted_table_still_resolves() {
    let mut db = Database::new();

    // Create table "Products" (as originally spelled)
    execute_create_table(&mut db, r#"CREATE TABLE "Products" (id INT)"#).unwrap();

    // A differently-cased quoted reference still resolves to the same table:
    // SQLite case-folds table names for lookup regardless of quoting (#5553).
    let result = execute_select(&db, r#"SELECT * FROM "products""#);
    assert!(result.is_ok(), "Case-folded quoted table reference should resolve: {:?}", result);
}

// ========================================================================
// Schema-Qualified Identifiers
// ========================================================================

#[test]
fn test_quoted_schema_and_table_names() {
    let mut db = Database::new();

    // Schema names are case-folded for lookup regardless of quoting, exactly
    // like table names (#5553), so a second CREATE SCHEMA that differs only
    // in case collides with the first.
    let stmt1 = Parser::parse_sql(r#"CREATE SCHEMA "mySchema""#).unwrap();
    if let Statement::CreateSchema(create_schema) = stmt1 {
        vibesql_executor::SchemaExecutor::execute_create_schema(&create_schema, &mut db).unwrap();
    }

    let stmt2 = Parser::parse_sql(r#"CREATE SCHEMA "MYSCHEMA""#).unwrap();
    if let Statement::CreateSchema(create_schema) = stmt2 {
        let err = vibesql_executor::SchemaExecutor::execute_create_schema(&create_schema, &mut db)
            .unwrap_err();
        assert!(format!("{:?}", err).contains("SchemaAlreadyExists"), "{:?}", err);
    }

    // A table created via one case-folded spelling of the schema is
    // reachable through any other spelling of the same schema.
    execute_create_table(&mut db, r#"CREATE TABLE "mySchema"."users" (id INT)"#).unwrap();
    execute_insert_sql(&mut db, r#"INSERT INTO "mySchema"."users" VALUES (1)"#).unwrap();
    execute_insert_sql(&mut db, r#"INSERT INTO "MYSCHEMA"."users" VALUES (2)"#).unwrap();

    let result1 = execute_select(&db, r#"SELECT * FROM "mySchema"."users""#).unwrap();
    let result2 = execute_select(&db, r#"SELECT * FROM "MYSCHEMA"."users""#).unwrap();

    assert_eq!(result1.len(), 2);
    assert_eq!(result2.len(), 2);
}

#[test]
fn test_mixed_quoted_unquoted_schema_table() {
    let mut db = Database::new();

    // Create schema with quoted name
    let stmt = Parser::parse_sql(r#"CREATE SCHEMA "myApp""#).unwrap();
    if let Statement::CreateSchema(create_schema) = stmt {
        vibesql_executor::SchemaExecutor::execute_create_schema(&create_schema, &mut db).unwrap();
    }

    // Create table: quoted schema, unquoted table (normalized to users)
    execute_create_table(&mut db, r#"CREATE TABLE "myApp".users (id INT)"#).unwrap();

    // Insert using SQL INSERT with schema-qualified name
    execute_insert_sql(&mut db, r#"INSERT INTO "myApp".users VALUES (42)"#).unwrap();

    // Query with quoted schema, unquoted table
    let result = execute_select(&db, r#"SELECT * FROM "myApp".users"#).unwrap();
    assert_eq!(result[0].values[0], SqlValue::Integer(42));
}
