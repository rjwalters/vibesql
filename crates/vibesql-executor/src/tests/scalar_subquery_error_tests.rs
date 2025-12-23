//! Scalar subquery error handling tests
//!
//! Tests for scalar subquery behavior and validation:
//! - Multiple rows returned: SQLite compatibility - returns first row (not an error)
//! - Multiple columns returned: column count violation (still an error)

use super::super::*;

#[test]
fn test_scalar_subquery_multiple_rows_returns_first() {
    // Test: Scalar subquery returns multiple rows - SQLite-compatible behavior
    // returns the first row's value instead of erroring
    // See: https://www.sqlite.org/lang_expr.html#scalar_subqueries
    let mut db = vibesql_storage::Database::new();

    // Create employees table
    let schema = vibesql_catalog::TableSchema::new(
        "employees".to_string(),
        vec![vibesql_catalog::ColumnSchema::new(
            "id".to_string(),
            vibesql_types::DataType::Integer,
            false,
        )],
    );
    db.create_table(schema).unwrap();

    // Insert multiple rows
    db.insert_row(
        "employees",
        vibesql_storage::Row::new(vec![vibesql_types::SqlValue::Integer(1)]),
    )
    .unwrap();
    db.insert_row(
        "employees",
        vibesql_storage::Row::new(vec![vibesql_types::SqlValue::Integer(2)]),
    )
    .unwrap();

    // Build subquery that returns multiple rows: SELECT id FROM employees
    let subquery = Box::new(vibesql_ast::SelectStmt {
        into_table: None,
        into_variables: None,
        with_clause: None,
        set_operation: None,
        values: None,
        distinct: false,
        select_list: vec![vibesql_ast::SelectItem::Expression {
            expr: vibesql_ast::Expression::ColumnRef(vibesql_ast::ColumnIdentifier::simple(
                "id", false,
            )),
            alias: None,
            source_text: None,
        }],
        from: Some(vibesql_ast::FromClause::Table {
            quoted: false,
            name: "employees".to_string(),
            alias: None,
            column_aliases: None,
        }),
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
    });

    // Build main query: SELECT (subquery) FROM employees LIMIT 1
    // (LIMIT 1 because we only need to check one result)
    let stmt = vibesql_ast::SelectStmt {
        into_table: None,
        into_variables: None,
        with_clause: None,
        set_operation: None,
        values: None,
        distinct: false,
        select_list: vec![vibesql_ast::SelectItem::Expression {
            expr: vibesql_ast::Expression::ScalarSubquery(subquery),
            alias: None,
            source_text: None,
        }],
        from: Some(vibesql_ast::FromClause::Table {
            quoted: false,
            name: "employees".to_string(),
            alias: None,
            column_aliases: None,
        }),
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: Some(vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(1))),
        offset: None,
    };

    let executor = SelectExecutor::new(&db);
    let result = executor.execute(&stmt);

    // SQLite-compatible: Should succeed and return the first row's value (1)
    assert!(result.is_ok(), "Should succeed with SQLite-compatible behavior");
    let rows = result.unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].values[0], vibesql_types::SqlValue::Integer(1));
}

#[test]
fn test_scalar_subquery_error_multiple_columns() {
    // Test: Scalar subquery returns multiple columns - should error
    let mut db = vibesql_storage::Database::new();

    // Create employees table with multiple columns
    let schema = vibesql_catalog::TableSchema::new(
        "employees".to_string(),
        vec![
            vibesql_catalog::ColumnSchema::new(
                "id".to_string(),
                vibesql_types::DataType::Integer,
                false,
            ),
            vibesql_catalog::ColumnSchema::new(
                "name".to_string(),
                vibesql_types::DataType::Varchar { max_length: Some(100) },
                false,
            ),
        ],
    );
    db.create_table(schema).unwrap();

    // Insert one row
    db.insert_row(
        "employees",
        vibesql_storage::Row::new(vec![
            vibesql_types::SqlValue::Integer(1),
            vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("Alice")),
        ]),
    )
    .unwrap();

    // Build subquery that returns multiple columns: SELECT id, name FROM employees
    let subquery = Box::new(vibesql_ast::SelectStmt {
        into_table: None,
        into_variables: None,
        with_clause: None,
        set_operation: None,
        values: None,
        distinct: false,
        select_list: vec![
            vibesql_ast::SelectItem::Expression {
                expr: vibesql_ast::Expression::ColumnRef(vibesql_ast::ColumnIdentifier::simple(
                    "id", false,
                )),
                alias: None,
                source_text: None,
            },
            vibesql_ast::SelectItem::Expression {
                expr: vibesql_ast::Expression::ColumnRef(vibesql_ast::ColumnIdentifier::simple(
                    "name", false,
                )),
                alias: None,
                source_text: None,
            },
        ],
        from: Some(vibesql_ast::FromClause::Table {
            quoted: false,
            name: "employees".to_string(),
            alias: None,
            column_aliases: None,
        }),
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
    });

    // Build main query: SELECT (subquery) FROM employees
    let stmt = vibesql_ast::SelectStmt {
        into_table: None,
        into_variables: None,
        with_clause: None,
        set_operation: None,
        values: None,
        distinct: false,
        select_list: vec![vibesql_ast::SelectItem::Expression {
            expr: vibesql_ast::Expression::ScalarSubquery(subquery),
            alias: None,
            source_text: None,
        }],
        from: Some(vibesql_ast::FromClause::Table {
            quoted: false,
            name: "employees".to_string(),
            alias: None,
            column_aliases: None,
        }),
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
    };

    let executor = SelectExecutor::new(&db);
    let result = executor.execute(&stmt);

    // Should error with SubqueryColumnCountMismatch
    assert!(result.is_err());
    match result.unwrap_err() {
        ExecutorError::SubqueryColumnCountMismatch { expected, actual } => {
            assert_eq!(expected, 1);
            assert_eq!(actual, 2);
        }
        _ => panic!("Expected SubqueryColumnCountMismatch error"),
    }
}
