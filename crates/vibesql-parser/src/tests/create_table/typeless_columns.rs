//! Tests for SQLite-style typeless column definitions
//!
//! SQLite allows column definitions without explicit data types. When no type
//! is specified, the column has BLOB affinity (any value can be stored).

use super::super::*;

// ========================================================================
// SQLite-style Typeless Column Tests
// ========================================================================

#[test]
fn test_parse_create_table_single_typeless_column() {
    // CREATE TABLE t1(x); - simplest case
    let result = Parser::parse_sql("CREATE TABLE t1(x);");
    assert!(result.is_ok(), "Failed to parse: {:?}", result);
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::CreateTable(create) => {
            assert_eq!(create.table_name, "T1");
            assert_eq!(create.columns.len(), 1);
            assert_eq!(create.columns[0].name, "X");
            // Typeless columns default to BLOB affinity
            assert_eq!(
                create.columns[0].data_type,
                vibesql_types::DataType::BinaryLargeObject
            );
        }
        _ => panic!("Expected CREATE TABLE statement"),
    }
}

#[test]
fn test_parse_create_table_multiple_typeless_columns() {
    // CREATE TABLE t1(a, b, c); - multiple typeless columns
    let result = Parser::parse_sql("CREATE TABLE t1(a, b, c);");
    assert!(result.is_ok(), "Failed to parse: {:?}", result);
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::CreateTable(create) => {
            assert_eq!(create.table_name, "T1");
            assert_eq!(create.columns.len(), 3);
            assert_eq!(create.columns[0].name, "A");
            assert_eq!(create.columns[1].name, "B");
            assert_eq!(create.columns[2].name, "C");
            // All columns default to BLOB affinity
            for col in &create.columns {
                assert_eq!(col.data_type, vibesql_types::DataType::BinaryLargeObject);
            }
        }
        _ => panic!("Expected CREATE TABLE statement"),
    }
}

#[test]
fn test_parse_create_table_typeless_with_primary_key() {
    // CREATE TABLE t1(x PRIMARY KEY); - typeless with constraint
    let result = Parser::parse_sql("CREATE TABLE t1(x PRIMARY KEY);");
    assert!(result.is_ok(), "Failed to parse: {:?}", result);
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::CreateTable(create) => {
            assert_eq!(create.table_name, "T1");
            assert_eq!(create.columns.len(), 1);
            assert_eq!(create.columns[0].name, "X");
            assert_eq!(
                create.columns[0].data_type,
                vibesql_types::DataType::BinaryLargeObject
            );
            // Should have PRIMARY KEY constraint
            assert!(create.columns[0]
                .constraints
                .iter()
                .any(|c| matches!(c.kind, vibesql_ast::ColumnConstraintKind::PrimaryKey)));
        }
        _ => panic!("Expected CREATE TABLE statement"),
    }
}

#[test]
fn test_parse_create_table_typeless_with_not_null() {
    // CREATE TABLE t1(x NOT NULL); - typeless with NOT NULL constraint
    let result = Parser::parse_sql("CREATE TABLE t1(x NOT NULL);");
    assert!(result.is_ok(), "Failed to parse: {:?}", result);
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::CreateTable(create) => {
            assert_eq!(create.columns[0].data_type, vibesql_types::DataType::BinaryLargeObject);
            assert!(!create.columns[0].nullable); // NOT NULL means not nullable
        }
        _ => panic!("Expected CREATE TABLE statement"),
    }
}

#[test]
fn test_parse_create_table_typeless_with_unique() {
    // CREATE TABLE t1(x UNIQUE); - typeless with UNIQUE constraint
    let result = Parser::parse_sql("CREATE TABLE t1(x UNIQUE);");
    assert!(result.is_ok(), "Failed to parse: {:?}", result);
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::CreateTable(create) => {
            assert_eq!(create.columns[0].data_type, vibesql_types::DataType::BinaryLargeObject);
            assert!(create.columns[0]
                .constraints
                .iter()
                .any(|c| matches!(c.kind, vibesql_ast::ColumnConstraintKind::Unique)));
        }
        _ => panic!("Expected CREATE TABLE statement"),
    }
}

#[test]
fn test_parse_create_table_typeless_with_default() {
    // CREATE TABLE t1(x DEFAULT 0); - typeless with DEFAULT
    let result = Parser::parse_sql("CREATE TABLE t1(x DEFAULT 0);");
    assert!(result.is_ok(), "Failed to parse: {:?}", result);
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::CreateTable(create) => {
            assert_eq!(create.columns[0].data_type, vibesql_types::DataType::BinaryLargeObject);
            assert!(create.columns[0].default_value.is_some());
        }
        _ => panic!("Expected CREATE TABLE statement"),
    }
}

#[test]
fn test_parse_create_table_mixed_typed_and_typeless() {
    // CREATE TABLE t1(a INTEGER, b, c TEXT); - mixed typed and typeless
    let result = Parser::parse_sql("CREATE TABLE t1(a INTEGER, b, c TEXT);");
    assert!(result.is_ok(), "Failed to parse: {:?}", result);
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::CreateTable(create) => {
            assert_eq!(create.columns.len(), 3);
            // First column has INTEGER type
            assert_eq!(create.columns[0].name, "A");
            assert_eq!(create.columns[0].data_type, vibesql_types::DataType::Integer);
            // Second column is typeless -> BLOB
            assert_eq!(create.columns[1].name, "B");
            assert_eq!(create.columns[1].data_type, vibesql_types::DataType::BinaryLargeObject);
            // Third column has TEXT type (maps to VARCHAR)
            assert_eq!(create.columns[2].name, "C");
            assert_eq!(
                create.columns[2].data_type,
                vibesql_types::DataType::Varchar { max_length: None }
            );
        }
        _ => panic!("Expected CREATE TABLE statement"),
    }
}

#[test]
fn test_parse_create_table_typeless_with_multiple_constraints() {
    // CREATE TABLE t1(x PRIMARY KEY NOT NULL UNIQUE);
    let result = Parser::parse_sql("CREATE TABLE t1(x PRIMARY KEY NOT NULL UNIQUE);");
    assert!(result.is_ok(), "Failed to parse: {:?}", result);
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::CreateTable(create) => {
            assert_eq!(create.columns[0].data_type, vibesql_types::DataType::BinaryLargeObject);
            assert!(!create.columns[0].nullable);
            let constraints = &create.columns[0].constraints;
            assert!(constraints
                .iter()
                .any(|c| matches!(c.kind, vibesql_ast::ColumnConstraintKind::PrimaryKey)));
            assert!(constraints.iter().any(|c| matches!(c.kind, vibesql_ast::ColumnConstraintKind::NotNull)));
            assert!(constraints.iter().any(|c| matches!(c.kind, vibesql_ast::ColumnConstraintKind::Unique)));
        }
        _ => panic!("Expected CREATE TABLE statement"),
    }
}

#[test]
fn test_parse_create_table_typeless_with_check_constraint() {
    // CREATE TABLE t1(x CHECK(x > 0));
    let result = Parser::parse_sql("CREATE TABLE t1(x CHECK(x > 0));");
    assert!(result.is_ok(), "Failed to parse: {:?}", result);
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::CreateTable(create) => {
            assert_eq!(create.columns[0].data_type, vibesql_types::DataType::BinaryLargeObject);
            assert!(create.columns[0]
                .constraints
                .iter()
                .any(|c| matches!(c.kind, vibesql_ast::ColumnConstraintKind::Check(_))));
        }
        _ => panic!("Expected CREATE TABLE statement"),
    }
}

#[test]
fn test_parse_create_table_typeless_with_references() {
    // CREATE TABLE t1(x REFERENCES other(id));
    let result = Parser::parse_sql("CREATE TABLE t1(x REFERENCES other(id));");
    assert!(result.is_ok(), "Failed to parse: {:?}", result);
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::CreateTable(create) => {
            assert_eq!(create.columns[0].data_type, vibesql_types::DataType::BinaryLargeObject);
            assert!(create.columns[0].constraints.iter().any(|c| matches!(
                &c.kind,
                vibesql_ast::ColumnConstraintKind::References { table, column, .. }
                    if table == "OTHER" && column == "ID"
            )));
        }
        _ => panic!("Expected CREATE TABLE statement"),
    }
}

#[test]
fn test_parse_create_table_complex_sqlite_pattern() {
    // Real-world SQLite pattern from test suite
    let result = Parser::parse_sql(
        "CREATE TABLE t1(w, x, y, z, PRIMARY KEY(w, x));",
    );
    assert!(result.is_ok(), "Failed to parse: {:?}", result);
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::CreateTable(create) => {
            assert_eq!(create.columns.len(), 4);
            // All columns are typeless
            for col in &create.columns {
                assert_eq!(col.data_type, vibesql_types::DataType::BinaryLargeObject);
            }
            // Should have table-level PRIMARY KEY constraint
            assert!(!create.table_constraints.is_empty());
            assert!(create.table_constraints.iter().any(|c| matches!(
                &c.kind,
                vibesql_ast::TableConstraintKind::PrimaryKey { columns } if columns.len() == 2
            )));
        }
        _ => panic!("Expected CREATE TABLE statement"),
    }
}

#[test]
fn test_parse_create_table_typeless_with_autoincrement() {
    // CREATE TABLE t1(x AUTOINCREMENT);
    let result = Parser::parse_sql("CREATE TABLE t1(x AUTOINCREMENT);");
    assert!(result.is_ok(), "Failed to parse: {:?}", result);
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::CreateTable(create) => {
            assert_eq!(create.columns[0].data_type, vibesql_types::DataType::BinaryLargeObject);
            assert!(create.columns[0]
                .constraints
                .iter()
                .any(|c| matches!(c.kind, vibesql_ast::ColumnConstraintKind::AutoIncrement)));
        }
        _ => panic!("Expected CREATE TABLE statement"),
    }
}
