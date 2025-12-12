//! Tests for schema DDL statements (CREATE SCHEMA, DROP SCHEMA, SET SCHEMA)

use vibesql_ast::SchemaElement;

use crate::Parser;

#[test]
fn test_create_schema_simple() {
    let sql = "CREATE SCHEMA myschema";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    if let Ok(vibesql_ast::Statement::CreateSchema(stmt)) = result {
        assert_eq!(stmt.schema_name, "myschema");
        assert_eq!(stmt.schema_elements.len(), 0);
        assert!(!stmt.if_not_exists);
    } else {
        panic!("Expected CreateSchema statement");
    }
}

#[test]
fn test_create_schema_with_embedded_table() {
    let sql = "CREATE SCHEMA s CREATE TABLE t (a INT)";
    let result = Parser::parse_sql(sql);

    match result {
        Ok(vibesql_ast::Statement::CreateSchema(stmt)) => {
            assert_eq!(stmt.schema_name, "s");
            assert_eq!(stmt.schema_elements.len(), 1);

            match &stmt.schema_elements[0] {
                SchemaElement::CreateTable(table_stmt) => {
                    assert_eq!(table_stmt.table_name, "t");
                    assert_eq!(table_stmt.columns.len(), 1);
                    assert_eq!(table_stmt.columns[0].name, "a");
                }
            }
        }
        Ok(other) => panic!("Expected CreateSchema, got: {:?}", other),
        Err(e) => panic!("Parse error: {}", e),
    }
}

#[test]
fn test_create_schema_with_multiple_tables() {
    let sql = "CREATE SCHEMA myschema CREATE TABLE t1 (a INT) CREATE TABLE t2 (b VARCHAR(10))";
    let result = Parser::parse_sql(sql);

    match result {
        Ok(vibesql_ast::Statement::CreateSchema(stmt)) => {
            assert_eq!(stmt.schema_name, "myschema");
            assert_eq!(stmt.schema_elements.len(), 2);

            // Check first table
            match &stmt.schema_elements[0] {
                SchemaElement::CreateTable(table_stmt) => {
                    assert_eq!(table_stmt.table_name, "t1");
                }
            }

            // Check second table
            match &stmt.schema_elements[1] {
                SchemaElement::CreateTable(table_stmt) => {
                    assert_eq!(table_stmt.table_name, "t2");
                }
            }
        }
        Ok(other) => panic!("Expected CreateSchema, got: {:?}", other),
        Err(e) => panic!("Parse error: {}", e),
    }
}
