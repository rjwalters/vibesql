//! POSITION string function tests
//!
//! Tests for SQL POSITION(substring IN string) function:
//! - Substring found (1-indexed position)
//! - Substring not found (returns 0)
//! - NULL substring handling
//! - NULL string handling

use super::super::super::*;

#[test]
fn test_position_found() {
    let db = vibesql_storage::Database::new();
    let executor = SelectExecutor::new(&db);

    // SELECT POSITION('world' IN 'hello world')
    let stmt = vibesql_ast::SelectStmt {
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![vibesql_ast::SelectItem::Expression {
            expr: vibesql_ast::Expression::Position {
                substring: Box::new(vibesql_ast::Expression::Literal(
                    vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("world")),
                )),
                string: Box::new(vibesql_ast::Expression::Literal(
                    vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("hello world")),
                )),
                character_unit: None,
            },
            alias: Some("pos".to_string()),
        }],
        from: None,
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
        into_table: None,
        into_variables: None,
    };

    let result = executor.execute(&stmt).unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].values[0], vibesql_types::SqlValue::Integer(7)); // Position is 1-indexed
}

#[test]
fn test_position_not_found() {
    let db = vibesql_storage::Database::new();
    let executor = SelectExecutor::new(&db);

    // SELECT POSITION('xyz' IN 'hello world')
    let stmt = vibesql_ast::SelectStmt {
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![vibesql_ast::SelectItem::Expression {
            expr: vibesql_ast::Expression::Position {
                substring: Box::new(vibesql_ast::Expression::Literal(
                    vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("xyz")),
                )),
                string: Box::new(vibesql_ast::Expression::Literal(
                    vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("hello world")),
                )),
                character_unit: None,
            },
            alias: Some("pos".to_string()),
        }],
        from: None,
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
        into_table: None,
        into_variables: None,
    };

    let result = executor.execute(&stmt).unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].values[0], vibesql_types::SqlValue::Integer(0)); // Not found returns 0
}

#[test]
fn test_position_null_substring() {
    let db = vibesql_storage::Database::new();
    let executor = SelectExecutor::new(&db);

    // SELECT POSITION(NULL IN 'hello world')
    let stmt = vibesql_ast::SelectStmt {
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![vibesql_ast::SelectItem::Expression {
            expr: vibesql_ast::Expression::Position {
                substring: Box::new(vibesql_ast::Expression::Literal(
                    vibesql_types::SqlValue::Null,
                )),
                string: Box::new(vibesql_ast::Expression::Literal(
                    vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("hello world")),
                )),
                character_unit: None,
            },
            alias: Some("pos".to_string()),
        }],
        from: None,
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
        into_table: None,
        into_variables: None,
    };

    let result = executor.execute(&stmt).unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].values[0], vibesql_types::SqlValue::Null);
}

#[test]
fn test_position_null_string() {
    let db = vibesql_storage::Database::new();
    let executor = SelectExecutor::new(&db);

    // SELECT POSITION('world' IN NULL)
    let stmt = vibesql_ast::SelectStmt {
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![vibesql_ast::SelectItem::Expression {
            expr: vibesql_ast::Expression::Position {
                substring: Box::new(vibesql_ast::Expression::Literal(
                    vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("world")),
                )),
                string: Box::new(vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Null)),
                character_unit: None,
            },
            alias: Some("pos".to_string()),
        }],
        from: None,
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
        into_table: None,
        into_variables: None,
    };

    let result = executor.execute(&stmt).unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].values[0], vibesql_types::SqlValue::Null);
}
