//! Tests for schema DDL statement parsing (SQL:1999 features)
//!
//! Tests for CREATE SCHEMA, DROP SCHEMA, and SET SCHEMA statements.

use vibesql_ast::Statement;
use vibesql_parser::Parser;

#[test]
fn test_create_schema_basic() {
    let sql = "CREATE SCHEMA test_schema";
    let stmt = Parser::parse_sql(sql).expect("Failed to parse CREATE SCHEMA");

    match stmt {
        Statement::CreateSchema(create_stmt) => {
            assert_eq!(create_stmt.schema_name, "test_schema");
            assert!(!create_stmt.if_not_exists);
            assert!(create_stmt.schema_elements.is_empty());
        }
        _ => panic!("Expected CreateSchema statement, got {:?}", stmt),
    }
}

#[test]
fn test_create_schema_if_not_exists() {
    let sql = "CREATE SCHEMA IF NOT EXISTS test_schema";
    let stmt = Parser::parse_sql(sql).expect("Failed to parse CREATE SCHEMA IF NOT EXISTS");

    match stmt {
        Statement::CreateSchema(create_stmt) => {
            assert_eq!(create_stmt.schema_name, "test_schema");
            assert!(create_stmt.if_not_exists);
            assert!(create_stmt.schema_elements.is_empty());
        }
        _ => panic!("Expected CreateSchema statement, got {:?}", stmt),
    }
}

#[test]
fn test_create_schema_qualified_name() {
    let sql = "CREATE SCHEMA catalog.test_schema";
    let stmt = Parser::parse_sql(sql).expect("Failed to parse CREATE SCHEMA with qualified name");

    match stmt {
        Statement::CreateSchema(create_stmt) => {
            // "catalog" is a keyword, so it's converted to uppercase per keyword handling
            assert_eq!(create_stmt.schema_name, "CATALOG.test_schema");
            assert!(!create_stmt.if_not_exists);
            assert!(create_stmt.schema_elements.is_empty());
        }
        _ => panic!("Expected CreateSchema statement, got {:?}", stmt),
    }
}

#[test]
fn test_create_schema_with_table() {
    let sql =
        "CREATE SCHEMA test_schema CREATE TABLE users (id INTEGER PRIMARY KEY, name VARCHAR(100))";
    let stmt = Parser::parse_sql(sql).expect("Failed to parse CREATE SCHEMA with table");

    match stmt {
        Statement::CreateSchema(create_stmt) => {
            assert_eq!(create_stmt.schema_name, "test_schema");
            assert!(!create_stmt.if_not_exists);
            assert_eq!(create_stmt.schema_elements.len(), 1);

            // Check that the table element is parsed
            match &create_stmt.schema_elements[0] {
                vibesql_ast::SchemaElement::CreateTable(table_stmt) => {
                    assert_eq!(table_stmt.table_name, "users");
                    assert_eq!(table_stmt.columns.len(), 2);
                }
            }
        }
        _ => panic!("Expected CreateSchema statement, got {:?}", stmt),
    }
}

#[test]
fn test_drop_schema_basic() {
    let sql = "DROP SCHEMA test_schema";
    let stmt = Parser::parse_sql(sql).expect("Failed to parse DROP SCHEMA");

    match stmt {
        Statement::DropSchema(drop_stmt) => {
            assert_eq!(drop_stmt.schema_name, "test_schema");
            assert!(!drop_stmt.if_exists);
            assert!(!drop_stmt.cascade); // RESTRICT is default
        }
        _ => panic!("Expected DropSchema statement, got {:?}", stmt),
    }
}

#[test]
fn test_drop_schema_if_exists() {
    let sql = "DROP SCHEMA IF EXISTS test_schema";
    let stmt = Parser::parse_sql(sql).expect("Failed to parse DROP SCHEMA IF EXISTS");

    match stmt {
        Statement::DropSchema(drop_stmt) => {
            assert_eq!(drop_stmt.schema_name, "test_schema");
            assert!(drop_stmt.if_exists);
            assert!(!drop_stmt.cascade);
        }
        _ => panic!("Expected DropSchema statement, got {:?}", stmt),
    }
}

#[test]
fn test_drop_schema_cascade() {
    let sql = "DROP SCHEMA test_schema CASCADE";
    let stmt = Parser::parse_sql(sql).expect("Failed to parse DROP SCHEMA CASCADE");

    match stmt {
        Statement::DropSchema(drop_stmt) => {
            assert_eq!(drop_stmt.schema_name, "test_schema");
            assert!(!drop_stmt.if_exists);
            assert!(drop_stmt.cascade);
        }
        _ => panic!("Expected DropSchema statement, got {:?}", stmt),
    }
}

#[test]
fn test_drop_schema_restrict() {
    let sql = "DROP SCHEMA test_schema RESTRICT";
    let stmt = Parser::parse_sql(sql).expect("Failed to parse DROP SCHEMA RESTRICT");

    match stmt {
        Statement::DropSchema(drop_stmt) => {
            assert_eq!(drop_stmt.schema_name, "test_schema");
            assert!(!drop_stmt.if_exists);
            assert!(!drop_stmt.cascade);
        }
        _ => panic!("Expected DropSchema statement, got {:?}", stmt),
    }
}

#[test]
fn test_drop_schema_if_exists_cascade() {
    let sql = "DROP SCHEMA IF EXISTS test_schema CASCADE";
    let stmt = Parser::parse_sql(sql).expect("Failed to parse DROP SCHEMA IF EXISTS CASCADE");

    match stmt {
        Statement::DropSchema(drop_stmt) => {
            assert_eq!(drop_stmt.schema_name, "test_schema");
            assert!(drop_stmt.if_exists);
            assert!(drop_stmt.cascade);
        }
        _ => panic!("Expected DropSchema statement, got {:?}", stmt),
    }
}

#[test]
fn test_drop_schema_qualified_name() {
    let sql = "DROP SCHEMA catalog.test_schema";
    let stmt = Parser::parse_sql(sql).expect("Failed to parse DROP SCHEMA with qualified name");

    match stmt {
        Statement::DropSchema(drop_stmt) => {
            // "catalog" is a keyword, so it's converted to uppercase per keyword handling
            assert_eq!(drop_stmt.schema_name, "CATALOG.test_schema");
            assert!(!drop_stmt.if_exists);
            assert!(!drop_stmt.cascade);
        }
        _ => panic!("Expected DropSchema statement, got {:?}", stmt),
    }
}

#[test]
fn test_set_schema() {
    let sql = "SET SCHEMA test_schema";
    match Parser::parse_sql(sql) {
        Ok(stmt) => match stmt {
            Statement::SetSchema(set_stmt) => {
                assert_eq!(set_stmt.schema_name, "test_schema");
            }
            _ => panic!("Expected SetSchema statement, got {:?}", stmt),
        },
        Err(e) => panic!("Failed to parse SET SCHEMA: {:?}", e),
    }
}

#[test]
fn test_set_schema_qualified() {
    let sql = "SET SCHEMA catalog.test_schema";
    let stmt = Parser::parse_sql(sql).expect("Failed to parse SET SCHEMA with qualified name");

    match stmt {
        Statement::SetSchema(set_stmt) => {
            // "catalog" is a keyword, so it's converted to uppercase per keyword handling
            assert_eq!(set_stmt.schema_name, "CATALOG.test_schema");
        }
        _ => panic!("Expected SetSchema statement, got {:?}", stmt),
    }
}

#[test]
fn test_set_schema_quoted() {
    let sql = "SET SCHEMA \"my schema\"";
    let stmt = Parser::parse_sql(sql).expect("Failed to parse SET SCHEMA with quoted name");

    match stmt {
        Statement::SetSchema(set_stmt) => {
            assert_eq!(set_stmt.schema_name, "my schema");
        }
        _ => panic!("Expected SetSchema statement, got {:?}", stmt),
    }
}

#[test]
fn test_schema_statement_case_insensitive() {
    // Test case insensitivity for keywords
    let sql_variants = vec![
        "create schema test",
        "CREATE SCHEMA test",
        "Create Schema test",
        "drop schema test",
        "DROP SCHEMA test",
        "Drop Schema test",
        "SET SCHEMA test", // SET must be uppercase for recognition
    ];

    for sql in sql_variants {
        Parser::parse_sql(sql).unwrap_or_else(|_| panic!("Failed to parse case variant: {}", sql));
    }
}

#[test]
fn test_schema_parse_errors() {
    // Test various parse error cases

    let invalid_statements = vec![
        "CREATE SCHEMA",        // Missing schema name
        "CREATE SCHEMA IF",     // Incomplete IF NOT EXISTS
        "CREATE SCHEMA IF NOT", // Incomplete IF NOT EXISTS
        "DROP SCHEMA",          // Missing schema name
        "DROP SCHEMA IF",       // Incomplete IF EXISTS
        "SET SCHEMA",           // Missing schema name
    ];

    for sql in invalid_statements {
        let result = Parser::parse_sql(sql);
        assert!(result.is_err(), "Expected parse error for: {}", sql);
    }
}

#[test]
fn test_create_schema_complex_example() {
    // Test a more complex schema with multiple tables
    let sql = r#"
        CREATE SCHEMA library
            CREATE TABLE books (
                id INTEGER PRIMARY KEY,
                title VARCHAR(255) NOT NULL,
                author_id INTEGER REFERENCES authors(id)
            )
            CREATE TABLE authors (
                id INTEGER PRIMARY KEY,
                name VARCHAR(100) NOT NULL
            )
    "#;

    let stmt = Parser::parse_sql(sql).expect("Failed to parse complex CREATE SCHEMA");

    match stmt {
        Statement::CreateSchema(create_stmt) => {
            assert_eq!(create_stmt.schema_name, "library");
            assert!(!create_stmt.if_not_exists);
            assert_eq!(create_stmt.schema_elements.len(), 2);

            // Check first table (books)
            match &create_stmt.schema_elements[0] {
                vibesql_ast::SchemaElement::CreateTable(table_stmt) => {
                    assert_eq!(table_stmt.table_name, "books");
                    assert_eq!(table_stmt.columns.len(), 3);
                }
            }

            // Check second table (authors)
            match &create_stmt.schema_elements[1] {
                vibesql_ast::SchemaElement::CreateTable(table_stmt) => {
                    assert_eq!(table_stmt.table_name, "authors");
                    assert_eq!(table_stmt.columns.len(), 2);
                }
            }
        }
        _ => panic!("Expected CreateSchema statement, got {:?}", stmt),
    }
}
