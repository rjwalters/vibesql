//! TRIM string function tests
//!
//! Tests for SQL TRIM function variations:
//! - Default BOTH trimming (spaces)
//! - LEADING trim (left side only)
//! - TRAILING trim (right side only)
//! - Custom character trimming
//! - NULL string and removal character handling

use super::super::super::*;

#[test]
fn test_trim_both_default() {
    let db = vibesql_storage::Database::new();
    let executor = SelectExecutor::new(&db);

    // SELECT TRIM('  hello  ')
    let stmt = vibesql_ast::SelectStmt {
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![vibesql_ast::SelectItem::Expression {
            expr: vibesql_ast::Expression::Trim {
                position: None,     // Defaults to Both
                removal_char: None, // Defaults to space
                string: Box::new(vibesql_ast::Expression::Literal(
                    vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("  hello  ")),
                )),
            },
            alias: Some("result".to_string()),
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
    assert_eq!(result[0].values[0], vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("hello")));
}

#[test]
fn test_trim_leading() {
    let db = vibesql_storage::Database::new();
    let executor = SelectExecutor::new(&db);

    // SELECT TRIM(LEADING FROM '  hello  ')
    let stmt = vibesql_ast::SelectStmt {
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![vibesql_ast::SelectItem::Expression {
            expr: vibesql_ast::Expression::Trim {
                position: Some(vibesql_ast::TrimPosition::Leading),
                removal_char: None,
                string: Box::new(vibesql_ast::Expression::Literal(
                    vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("  hello  ")),
                )),
            },
            alias: Some("result".to_string()),
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
    assert_eq!(result[0].values[0], vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("hello  ")));
}

#[test]
fn test_trim_trailing() {
    let db = vibesql_storage::Database::new();
    let executor = SelectExecutor::new(&db);

    // SELECT TRIM(TRAILING FROM '  hello  ')
    let stmt = vibesql_ast::SelectStmt {
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![vibesql_ast::SelectItem::Expression {
            expr: vibesql_ast::Expression::Trim {
                position: Some(vibesql_ast::TrimPosition::Trailing),
                removal_char: None,
                string: Box::new(vibesql_ast::Expression::Literal(
                    vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("  hello  ")),
                )),
            },
            alias: Some("result".to_string()),
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
    assert_eq!(result[0].values[0], vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("  hello")));
}

#[test]
fn test_trim_custom_char() {
    let db = vibesql_storage::Database::new();
    let executor = SelectExecutor::new(&db);

    // SELECT TRIM('x' FROM 'xxxhelloxxx')
    let stmt = vibesql_ast::SelectStmt {
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![vibesql_ast::SelectItem::Expression {
            expr: vibesql_ast::Expression::Trim {
                position: None,
                removal_char: Some(Box::new(vibesql_ast::Expression::Literal(
                    vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("x")),
                ))),
                string: Box::new(vibesql_ast::Expression::Literal(
                    vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("xxxhelloxxx")),
                )),
            },
            alias: Some("result".to_string()),
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
    assert_eq!(result[0].values[0], vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("hello")));
}

#[test]
fn test_trim_null_string() {
    let db = vibesql_storage::Database::new();
    let executor = SelectExecutor::new(&db);

    // SELECT TRIM(NULL)
    let stmt = vibesql_ast::SelectStmt {
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![vibesql_ast::SelectItem::Expression {
            expr: vibesql_ast::Expression::Trim {
                position: None,
                removal_char: None,
                string: Box::new(vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Null)),
            },
            alias: Some("result".to_string()),
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
fn test_trim_null_removal_char() {
    let db = vibesql_storage::Database::new();
    let executor = SelectExecutor::new(&db);

    // SELECT TRIM(NULL FROM 'hello')
    let stmt = vibesql_ast::SelectStmt {
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![vibesql_ast::SelectItem::Expression {
            expr: vibesql_ast::Expression::Trim {
                position: None,
                removal_char: Some(Box::new(vibesql_ast::Expression::Literal(
                    vibesql_types::SqlValue::Null,
                ))),
                string: Box::new(vibesql_ast::Expression::Literal(
                    vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("hello")),
                )),
            },
            alias: Some("result".to_string()),
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
