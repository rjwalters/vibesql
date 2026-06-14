//! CREATE TABLE executor tests
//!
//! Tests for basic CREATE TABLE functionality including:
//! - Simple table creation
//! - Multiple data types
//! - Nullable columns
//! - Table already exists error handling
//! - Empty column lists
//! - Multiple table creation
//! - Special characters in names
//! - Case sensitivity
//! - Spatial types (POINT, POLYGON, MULTIPOLYGON)

use vibesql_ast::{ColumnDef, CreateTableStmt};
use vibesql_storage::Database;
use vibesql_types::DataType;

use crate::CreateTableExecutor;

#[test]
fn test_create_simple_table() {
    let mut db = Database::new();

    let stmt = CreateTableStmt {
        temporary: false,
        if_not_exists: false,
        table_name: "users".to_string(),
        columns: vec![
            ColumnDef {
                name: "id".to_string(),
                data_type: DataType::Integer,
                nullable: false,
                constraints: vec![],
                default_value: None,
                comment: None,
                generated_expr: None,
                is_exact_integer_type: false,
            },
            ColumnDef {
                name: "name".to_string(),
                data_type: DataType::Varchar { max_length: Some(255) },
                nullable: true,
                constraints: vec![],
                default_value: None,
                comment: None,
                generated_expr: None,
                is_exact_integer_type: false,
            },
        ],
        table_constraints: vec![],
        table_options: vec![],
        quoted: false,
        name_source: None,
        as_query: None,
        without_rowid: false,
    };

    let result = CreateTableExecutor::execute(&stmt, &mut db);
    assert!(result.is_ok());
    assert_eq!(
        result.unwrap(),
        format!(
            "Table 'users' created successfully in schema '{}'",
            vibesql_catalog::DEFAULT_SCHEMA
        )
    );

    // Verify table exists in catalog
    assert!(db.catalog.table_exists("users"));

    // Verify table is accessible from storage
    assert!(db.get_table("users").is_some());
}

#[test]
fn test_create_table_with_multiple_types() {
    let mut db = Database::new();

    let stmt = CreateTableStmt {
        temporary: false,
        if_not_exists: false,
        table_name: "products".to_string(),
        columns: vec![
            ColumnDef {
                name: "product_id".to_string(),
                data_type: DataType::Integer,
                nullable: false,
                constraints: vec![],
                default_value: None,
                comment: None,
                generated_expr: None,
                is_exact_integer_type: false,
            },
            ColumnDef {
                name: "name".to_string(),
                data_type: DataType::Varchar { max_length: Some(100) },
                nullable: false,
                constraints: vec![],
                default_value: None,
                comment: None,
                generated_expr: None,
                is_exact_integer_type: false,
            },
            ColumnDef {
                name: "price".to_string(),
                data_type: DataType::Integer, /* Using Integer for price (could be Decimal in
                                               * future) */
                nullable: false,
                constraints: vec![],
                default_value: None,
                comment: None,
                generated_expr: None,
                is_exact_integer_type: false,
            },
            ColumnDef {
                name: "in_stock".to_string(),
                data_type: DataType::Boolean,
                nullable: false,
                constraints: vec![],
                default_value: None,
                comment: None,
                generated_expr: None,
                is_exact_integer_type: false,
            },
            ColumnDef {
                name: "description".to_string(),
                data_type: DataType::Varchar { max_length: Some(500) },
                nullable: true, // Optional field
                constraints: vec![],
                default_value: None,
                comment: None,
                generated_expr: None,
                is_exact_integer_type: false,
            },
        ],
        table_constraints: vec![],
        table_options: vec![],
        quoted: false,
        name_source: None,
        as_query: None,
        without_rowid: false,
    };

    let result = CreateTableExecutor::execute(&stmt, &mut db);
    assert!(result.is_ok());

    // Verify schema correctness
    let schema = db.catalog.get_table("products");
    assert!(schema.is_some());
    let schema = schema.unwrap();
    assert_eq!(schema.column_count(), 5);
    assert!(!schema.get_column("product_id").unwrap().nullable);
    assert!(schema.get_column("description").unwrap().nullable);
}

#[test]
fn test_create_table_already_exists() {
    let mut db = Database::new();

    let stmt = CreateTableStmt {
        temporary: false,
        if_not_exists: false,
        table_name: "users".to_string(),
        columns: vec![ColumnDef {
            name: "id".to_string(),
            data_type: DataType::Integer,
            nullable: false,
            constraints: vec![],
            default_value: None,
            comment: None,
            generated_expr: None,
            is_exact_integer_type: false,
        }],
        table_constraints: vec![],
        table_options: vec![],
        quoted: false,
        name_source: None,
        as_query: None,
        without_rowid: false,
    };

    // First creation succeeds
    let result = CreateTableExecutor::execute(&stmt, &mut db);
    assert!(result.is_ok());

    // Second creation fails
    let result = CreateTableExecutor::execute(&stmt, &mut db);
    assert!(result.is_err());
    assert!(matches!(result, Err(crate::errors::ExecutorError::TableAlreadyExists(_))));
}

/// Helper: parse a CREATE TABLE statement string and return the typed AST.
fn parse_create_table(sql: &str) -> CreateTableStmt {
    match vibesql_parser::Parser::parse_sql(sql).expect("parse failed") {
        vibesql_ast::Statement::CreateTable(stmt) => stmt,
        other => panic!("Expected CreateTable, got {:?}", other),
    }
}

#[test]
fn test_duplicate_table_already_exists_echoes_source_quoting() {
    // SQLite (3.51.0) echoes the table name *exactly as written* in the
    // "already exists" error, preserving its quoting form and casing:
    //   CREATE TABLE  tbl1  -> table tbl1 already exists
    //   CREATE TABLE "tbl1" -> table "tbl1" already exists
    //   CREATE TABLE [tbl1] -> table [tbl1] already exists
    //   CREATE TABLE `tbl1` -> table `tbl1` already exists
    //   CREATE TABLE "TBL1" -> table "TBL1" already exists
    //
    // VibeSQL surfaces this via `Table '<echoed>' already exists`; the TCL
    // conformance shim rewrites it to SQLite's lowercase-wrapper form while
    // preserving `<echoed>`. We assert the raw VibeSQL message here. The error
    // echoes the *failing* (second) statement's source spelling, regardless of
    // how the first (existing) table was spelled.
    // (first_sql, dup_sql, expected_message). The dup statement collides with
    // the first under VibeSQL's identifier-equality rules, and the error echoes
    // the *dup* statement's verbatim spelling.
    let cases = [
        ("CREATE TABLE tbl1 (a)", "CREATE TABLE tbl1 (a)", "Table 'tbl1' already exists"),
        ("CREATE TABLE tbl1 (a)", "CREATE TABLE \"tbl1\" (a)", "Table '\"tbl1\"' already exists"),
        ("CREATE TABLE tbl1 (a)", "CREATE TABLE [tbl1] (a)", "Table '[tbl1]' already exists"),
        ("CREATE TABLE tbl1 (a)", "CREATE TABLE `tbl1` (a)", "Table '`tbl1`' already exists"),
        // Casing is preserved verbatim in the echoed name.
        (
            "CREATE TABLE \"TBL1\" (a)",
            "CREATE TABLE \"TBL1\" (a)",
            "Table '\"TBL1\"' already exists",
        ),
    ];

    for (first_sql, dup_sql, expected) in cases {
        let mut db = Database::new();

        // First create succeeds and registers the table.
        let first = parse_create_table(first_sql);
        CreateTableExecutor::execute(&first, &mut db).expect("first create should succeed");

        // Re-creating with the (possibly quoted) duplicate name errors, echoing
        // the source quoting form of the failing statement.
        let dup = parse_create_table(dup_sql);
        let err = CreateTableExecutor::execute(&dup, &mut db)
            .expect_err("duplicate table must be rejected");
        assert_eq!(err.to_string(), expected, "for SQL: {dup_sql}");
    }
}

#[test]
fn test_duplicate_table_as_select_echoes_source_quoting() {
    // CREATE TABLE ... AS SELECT over an existing name also preserves the
    // source quoting form (sqlite3 3.51.0).
    let mut db = Database::new();
    let first = parse_create_table("CREATE TABLE tbl1 (a)");
    CreateTableExecutor::execute(&first, &mut db).expect("first create should succeed");

    let dup = parse_create_table("CREATE TABLE \"tbl1\" AS SELECT 1");
    let err =
        CreateTableExecutor::execute(&dup, &mut db).expect_err("duplicate CTAS must be rejected");
    assert_eq!(err.to_string(), "Table '\"tbl1\"' already exists");
}

#[test]
fn test_duplicate_table_programmatic_ast_falls_back_to_normalized_name() {
    // An AST built without source spelling (name_source == None) falls back to
    // the schema-qualified normalized name in the error, preserving the legacy
    // behavior the TCL shim's schema-prefix-strip handles.
    let mut db = Database::new();
    let mut stmt = parse_create_table("CREATE TABLE tbl1 (a)");
    stmt.name_source = None;
    CreateTableExecutor::execute(&stmt, &mut db).expect("first create should succeed");

    let err =
        CreateTableExecutor::execute(&stmt, &mut db).expect_err("duplicate must be rejected");
    // schema-qualified fallback (e.g. "public.tbl1" / "main.tbl1").
    let msg = err.to_string();
    assert!(msg.ends_with("tbl1' already exists"), "unexpected fallback message: {msg}");
    assert!(msg.contains('.'), "expected schema-qualified fallback: {msg}");
}

#[test]
fn test_create_table_with_nullable_columns() {
    let mut db = Database::new();

    let stmt = CreateTableStmt {
        temporary: false,
        if_not_exists: false,
        table_name: "employees".to_string(),
        columns: vec![
            ColumnDef {
                name: "id".to_string(),
                data_type: DataType::Integer,
                nullable: false,
                constraints: vec![],
                default_value: None,
                comment: None,
                generated_expr: None,
                is_exact_integer_type: false,
            },
            ColumnDef {
                name: "middle_name".to_string(),
                data_type: DataType::Varchar { max_length: Some(50) },
                nullable: true, // Nullable field
                constraints: vec![],
                default_value: None,
                comment: None,
                generated_expr: None,
                is_exact_integer_type: false,
            },
            ColumnDef {
                name: "manager_id".to_string(),
                data_type: DataType::Integer,
                nullable: true, // Nullable foreign key
                constraints: vec![],
                default_value: None,
                comment: None,
                generated_expr: None,
                is_exact_integer_type: false,
            },
        ],
        table_constraints: vec![],
        table_options: vec![],
        quoted: false,
        name_source: None,
        as_query: None,
        without_rowid: false,
    };

    let result = CreateTableExecutor::execute(&stmt, &mut db);
    assert!(result.is_ok());

    // Verify nullable attribute is preserved
    let schema = db.catalog.get_table("employees").unwrap();
    assert!(!schema.get_column("id").unwrap().nullable);
    assert!(schema.get_column("middle_name").unwrap().nullable);
    assert!(schema.get_column("manager_id").unwrap().nullable);
}

#[test]
fn test_create_table_empty_columns_list() {
    let mut db = Database::new();

    let stmt = CreateTableStmt {
        temporary: false,
        if_not_exists: false,
        table_name: "empty_table".to_string(),
        columns: vec![], // Empty columns
        table_constraints: vec![],
        table_options: vec![],
        quoted: false,
        name_source: None,
        as_query: None,
        without_rowid: false,
    };

    // Should succeed (though not very useful)
    let result = CreateTableExecutor::execute(&stmt, &mut db);
    assert!(result.is_ok());

    let schema = db.catalog.get_table("empty_table").unwrap();
    assert_eq!(schema.column_count(), 0);
}

#[test]
fn test_create_multiple_tables() {
    let mut db = Database::new();

    // Create first table
    let stmt1 = CreateTableStmt {
        temporary: false,
        if_not_exists: false,
        table_name: "customers".to_string(),
        columns: vec![ColumnDef {
            name: "customer_id".to_string(),
            data_type: DataType::Integer,
            nullable: false,
            constraints: vec![],
            default_value: None,
            comment: None,
            generated_expr: None,
            is_exact_integer_type: false,
        }],
        table_constraints: vec![],
        table_options: vec![],
        quoted: false,
        name_source: None,
        as_query: None,
        without_rowid: false,
    };
    CreateTableExecutor::execute(&stmt1, &mut db).unwrap();

    // Create second table
    let stmt2 = CreateTableStmt {
        temporary: false,
        if_not_exists: false,
        table_name: "orders".to_string(),
        columns: vec![ColumnDef {
            name: "order_id".to_string(),
            data_type: DataType::Integer,
            nullable: false,
            constraints: vec![],
            default_value: None,
            comment: None,
            generated_expr: None,
            is_exact_integer_type: false,
        }],
        table_constraints: vec![],
        table_options: vec![],
        quoted: false,
        name_source: None,
        as_query: None,
        without_rowid: false,
    };
    CreateTableExecutor::execute(&stmt2, &mut db).unwrap();

    // Verify both tables exist
    assert!(db.catalog.table_exists("customers"));
    assert!(db.catalog.table_exists("orders"));
    assert_eq!(db.list_tables().len(), 2);
}

#[test]
fn test_create_table_with_special_characters_in_name() {
    let mut db = Database::new();

    // Test with underscores (common case)
    let stmt = CreateTableStmt {
        temporary: false,
        if_not_exists: false,
        table_name: "user_profiles".to_string(),
        columns: vec![ColumnDef {
            name: "profile_id".to_string(),
            data_type: DataType::Integer,
            nullable: false,
            constraints: vec![],
            default_value: None,
            comment: None,
            generated_expr: None,
            is_exact_integer_type: false,
        }],
        table_constraints: vec![],
        table_options: vec![],
        quoted: false,
        name_source: None,
        as_query: None,
        without_rowid: false,
    };

    let result = CreateTableExecutor::execute(&stmt, &mut db);
    assert!(result.is_ok());
    assert!(db.catalog.table_exists("user_profiles"));
}

/// Helper: build a single-`id`-column CREATE TABLE statement.
fn create_table_stmt(name: &str, quoted: bool) -> CreateTableStmt {
    CreateTableStmt {
        temporary: false,
        if_not_exists: false,
        table_name: name.to_string(),
        columns: vec![ColumnDef {
            name: "id".to_string(),
            data_type: DataType::Integer,
            nullable: false,
            constraints: vec![],
            default_value: None,
            comment: None,
            generated_expr: None,
            is_exact_integer_type: false,
        }],
        table_constraints: vec![],
        table_options: vec![],
        quoted,
        name_source: None,
        as_query: None,
        without_rowid: false,
    }
}

#[test]
fn test_create_table_case_sensitivity() {
    // SQLite case-folds identifiers regardless of quoting (issue #5553):
    // `CREATE TABLE "Users"` then `CREATE TABLE users` must collide.
    let mut db = Database::new();

    // Create "Users" as a quoted (mixed-case) identifier.
    CreateTableExecutor::execute(&create_table_stmt("Users", true), &mut db).unwrap();

    // A differing-case unquoted create must be rejected (already exists).
    let result = CreateTableExecutor::execute(&create_table_stmt("users", false), &mut db);
    assert!(result.is_err(), "differing-case unquoted CREATE should collide with quoted table");

    // A differing-case quoted create must also be rejected.
    let result = CreateTableExecutor::execute(&create_table_stmt("USERS", true), &mut db);
    assert!(result.is_err(), "differing-case quoted CREATE should collide too");

    // Exactly one table exists, resolvable by every case/quoting variant.
    assert!(db.catalog.table_exists("users"));
    assert!(db.catalog.table_exists("USERS"));
    assert!(db.catalog.table_exists("Users"));
    assert_eq!(db.catalog.list_tables().len(), 1);
}

#[test]
fn test_create_table_quoted_mixed_case_resolves_and_preserves_spelling() {
    // Mixed-case quoted create resolves via any case AND echoes original case
    // in the catalog (sqlite_master-style original-case preservation).
    let mut db = Database::new();

    CreateTableExecutor::execute(&create_table_stmt("MixedCase", true), &mut db).unwrap();

    // Resolvable by differing case and quoting form.
    assert!(db.catalog.table_exists("mixedcase"));
    assert!(db.catalog.table_exists("MIXEDCASE"));
    assert!(db.catalog.get_table("MixedCase").is_some());

    // Original spelling preserved on the stored table schema.
    let table = db.catalog.get_table("mixedcase").expect("table resolvable lowercase");
    assert_eq!(table.name, "MixedCase", "original declared case must be preserved");
}

#[test]
fn test_create_table_with_spatial_types() {
    // Test spatial data types (SQL/MM standard) - Issue #818
    // These are parsed as UserDefined types and should be accepted by executor
    let mut db = Database::new();

    let stmt = CreateTableStmt {
        temporary: false,
        if_not_exists: false,
        table_name: "spatial_table".to_string(),
        columns: vec![
            ColumnDef {
                name: "id".to_string(),
                data_type: DataType::Integer,
                nullable: false,
                constraints: vec![],
                default_value: None,
                comment: None,
                generated_expr: None,
                is_exact_integer_type: false,
            },
            ColumnDef {
                name: "location".to_string(),
                data_type: DataType::UserDefined { type_name: "POINT".to_string() },
                nullable: true,
                constraints: vec![],
                default_value: None,
                comment: None,
                generated_expr: None,
                is_exact_integer_type: false,
            },
            ColumnDef {
                name: "area".to_string(),
                data_type: DataType::UserDefined { type_name: "POLYGON".to_string() },
                nullable: true,
                constraints: vec![],
                default_value: None,
                comment: None,
                generated_expr: None,
                is_exact_integer_type: false,
            },
            ColumnDef {
                name: "regions".to_string(),
                data_type: DataType::UserDefined { type_name: "MULTIPOLYGON".to_string() },
                nullable: true,
                constraints: vec![],
                default_value: None,
                comment: None,
                generated_expr: None,
                is_exact_integer_type: false,
            },
        ],
        table_constraints: vec![],
        table_options: vec![],
        quoted: false,
        name_source: None,
        as_query: None,
        without_rowid: false,
    };

    let result = CreateTableExecutor::execute(&stmt, &mut db);
    assert!(result.is_ok(), "Should accept spatial types as UserDefined types");

    // Verify table exists and has correct schema
    let schema = db.catalog.get_table("spatial_table");
    assert!(schema.is_some());
    let schema = schema.unwrap();
    assert_eq!(schema.column_count(), 4);

    // Verify spatial type columns exist
    assert!(schema.get_column("location").is_some());
    assert!(schema.get_column("area").is_some());
    assert!(schema.get_column("regions").is_some());
}

#[test]
fn test_create_table_multipolygon_sqllogictest() {
    // Test the exact scenario from SQLLogicTest - Issue #818
    let mut db = Database::new();

    let stmt = CreateTableStmt {
        temporary: false,
        if_not_exists: false,
        table_name: "t1710a".to_string(),
        columns: vec![
            ColumnDef {
                name: "c1".to_string(),
                data_type: DataType::UserDefined { type_name: "MULTIPOLYGON".to_string() },
                nullable: true,
                constraints: vec![],
                default_value: None,
                comment: Some("text155459".to_string()),
                generated_expr: None,
                is_exact_integer_type: false,
            },
            ColumnDef {
                name: "c2".to_string(),
                data_type: DataType::UserDefined { type_name: "MULTIPOLYGON".to_string() },
                nullable: true,
                constraints: vec![],
                default_value: None,
                comment: Some("text155461".to_string()),
                generated_expr: None,
                is_exact_integer_type: false,
            },
        ],
        table_constraints: vec![],
        table_options: vec![],
        quoted: false,
        name_source: None,
        as_query: None,
        without_rowid: false,
    };

    let result = CreateTableExecutor::execute(&stmt, &mut db);
    assert!(
        result.is_ok(),
        "Should create table with MULTIPOLYGON columns (SQLLogicTest conformance)"
    );

    // Verify table was created successfully
    assert!(db.catalog.table_exists("t1710a"));
    let schema = db.catalog.get_table("t1710a").unwrap();
    assert_eq!(schema.column_count(), 2);
}
