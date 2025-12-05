use vibesql_ast::{Assignment, Expression};
use vibesql_catalog::{ColumnSchema, TableSchema};
use vibesql_executor::UpdateExecutor;
use vibesql_storage::{Database, Row};
use vibesql_types::{DataType, SqlValue};

// UPDATE WHERE with Subquery Tests (Issue #353)

#[test]
fn test_update_where_in_subquery() {
    let mut db = Database::new();

    // Create employees table
    let schema = TableSchema::new(
        "employees".to_string(),
        vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new("salary".to_string(), DataType::Integer, false),
            ColumnSchema::new("dept_id".to_string(), DataType::Integer, false),
        ],
    );
    db.create_table(schema).unwrap();

    // Insert test data
    db.insert_row(
        "employees",
        Row::new(vec![SqlValue::Integer(1), SqlValue::Integer(50000), SqlValue::Integer(10)]),
    )
    .unwrap();
    db.insert_row(
        "employees",
        Row::new(vec![SqlValue::Integer(2), SqlValue::Integer(60000), SqlValue::Integer(20)]),
    )
    .unwrap();
    db.insert_row(
        "employees",
        Row::new(vec![SqlValue::Integer(3), SqlValue::Integer(70000), SqlValue::Integer(10)]),
    )
    .unwrap();

    // Create departments table
    let dept_schema = TableSchema::new(
        "active_depts".to_string(),
        vec![ColumnSchema::new("dept_id".to_string(), DataType::Integer, false)],
    );
    db.create_table(dept_schema).unwrap();
    db.insert_row("active_depts", Row::new(vec![SqlValue::Integer(10)])).unwrap();

    // Build subquery: SELECT dept_id FROM active_depts
    let subquery = Box::new(vibesql_ast::SelectStmt {
        with_clause: None,
        distinct: false,
        select_list: vec![vibesql_ast::SelectItem::Expression {
            expr: Expression::ColumnRef { table: None, column: "dept_id".to_string() },
            alias: None,
        }],
        into_table: None,
        into_variables: None,
        from: Some(vibesql_ast::FromClause::Table {
            name: "active_depts".to_string(),
            alias: None,
            column_aliases: None,
        }),
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
        set_operation: None,
    });

    // UPDATE employees SET salary = 80000 WHERE dept_id IN (SELECT dept_id FROM active_depts)
    let stmt = vibesql_ast::UpdateStmt {
        table_name: "employees".to_string(),
        assignments: vec![Assignment {
            column: "salary".to_string(),
            value: Expression::Literal(SqlValue::Integer(80000)),
        }],
        where_clause: Some(vibesql_ast::WhereClause::Condition(Expression::In {
            expr: Box::new(Expression::ColumnRef { table: None, column: "dept_id".to_string() }),
            subquery,
            negated: false,
        })),
    };

    let count = UpdateExecutor::execute(&stmt, &mut db).unwrap();
    assert_eq!(count, 2); // Employees 1 and 3 in dept 10

    // Verify updates
    let table = db.get_table("employees").unwrap();
    let rows: Vec<&Row> = table.scan().iter().collect();
    assert_eq!(rows[0].get(1).unwrap(), &SqlValue::Integer(80000)); // Updated
    assert_eq!(rows[1].get(1).unwrap(), &SqlValue::Integer(60000)); // Not updated
    assert_eq!(rows[2].get(1).unwrap(), &SqlValue::Integer(80000)); // Updated
}

#[test]
fn test_update_where_not_in_subquery() {
    let mut db = Database::new();

    // Create employees table
    let schema = TableSchema::new(
        "employees".to_string(),
        vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new("active".to_string(), DataType::Boolean, false),
            ColumnSchema::new("dept_id".to_string(), DataType::Integer, false),
        ],
    );
    db.create_table(schema).unwrap();

    db.insert_row(
        "employees",
        Row::new(vec![SqlValue::Integer(1), SqlValue::Boolean(true), SqlValue::Integer(10)]),
    )
    .unwrap();
    db.insert_row(
        "employees",
        Row::new(vec![SqlValue::Integer(2), SqlValue::Boolean(true), SqlValue::Integer(20)]),
    )
    .unwrap();

    // Create active departments
    let dept_schema = TableSchema::new(
        "active_depts".to_string(),
        vec![ColumnSchema::new("dept_id".to_string(), DataType::Integer, false)],
    );
    db.create_table(dept_schema).unwrap();
    db.insert_row("active_depts", Row::new(vec![SqlValue::Integer(10)])).unwrap();

    // Subquery
    let subquery = Box::new(vibesql_ast::SelectStmt {
        with_clause: None,
        distinct: false,
        select_list: vec![vibesql_ast::SelectItem::Expression {
            expr: Expression::ColumnRef { table: None, column: "dept_id".to_string() },
            alias: None,
        }],
        into_table: None,
        into_variables: None,
        from: Some(vibesql_ast::FromClause::Table {
            name: "active_depts".to_string(),
            alias: None,
            column_aliases: None,
        }),
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
        set_operation: None,
    });

    // UPDATE employees SET active = FALSE WHERE dept_id NOT IN (SELECT dept_id FROM active_depts)
    let stmt = vibesql_ast::UpdateStmt {
        table_name: "employees".to_string(),
        assignments: vec![Assignment {
            column: "active".to_string(),
            value: Expression::Literal(SqlValue::Boolean(false)),
        }],
        where_clause: Some(vibesql_ast::WhereClause::Condition(Expression::In {
            expr: Box::new(Expression::ColumnRef { table: None, column: "dept_id".to_string() }),
            subquery,
            negated: true,
        })),
    };

    let count = UpdateExecutor::execute(&stmt, &mut db).unwrap();
    assert_eq!(count, 1); // Employee 2 not in active depts

    // Verify
    let table = db.get_table("employees").unwrap();
    let rows: Vec<&Row> = table.scan().iter().collect();
    assert_eq!(rows[0].get(1).unwrap(), &SqlValue::Boolean(true)); // Not updated
    assert_eq!(rows[1].get(1).unwrap(), &SqlValue::Boolean(false)); // Updated
}

#[test]
fn test_update_where_subquery_empty_result() {
    let mut db = Database::new();

    // Create employees table
    let schema = TableSchema::new(
        "employees".to_string(),
        vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new("dept_id".to_string(), DataType::Integer, false),
            ColumnSchema::new("active".to_string(), DataType::Boolean, false),
        ],
    );
    db.create_table(schema).unwrap();

    db.insert_row(
        "employees",
        Row::new(vec![SqlValue::Integer(1), SqlValue::Integer(10), SqlValue::Boolean(true)]),
    )
    .unwrap();

    // Create empty table
    let dept_schema = TableSchema::new(
        "inactive_depts".to_string(),
        vec![ColumnSchema::new("dept_id".to_string(), DataType::Integer, false)],
    );
    db.create_table(dept_schema).unwrap();

    // Subquery returns empty result
    let subquery = Box::new(vibesql_ast::SelectStmt {
        with_clause: None,
        distinct: false,
        select_list: vec![vibesql_ast::SelectItem::Expression {
            expr: Expression::ColumnRef { table: None, column: "dept_id".to_string() },
            alias: None,
        }],
        into_table: None,
        into_variables: None,
        from: Some(vibesql_ast::FromClause::Table {
            name: "inactive_depts".to_string(),
            alias: None,
            column_aliases: None,
        }),
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
        set_operation: None,
    });

    // UPDATE employees SET active = FALSE WHERE dept_id IN (SELECT dept_id FROM inactive_depts)
    let stmt = vibesql_ast::UpdateStmt {
        table_name: "employees".to_string(),
        assignments: vec![Assignment {
            column: "active".to_string(),
            value: Expression::Literal(SqlValue::Boolean(false)),
        }],
        where_clause: Some(vibesql_ast::WhereClause::Condition(Expression::In {
            expr: Box::new(Expression::ColumnRef { table: None, column: "dept_id".to_string() }),
            subquery,
            negated: false,
        })),
    };

    let count = UpdateExecutor::execute(&stmt, &mut db).unwrap();
    assert_eq!(count, 0); // No rows updated (empty IN list)

    let table = db.get_table("employees").unwrap();
    let rows: Vec<&Row> = table.scan().iter().collect();
    assert_eq!(rows[0].get(2).unwrap(), &SqlValue::Boolean(true)); // Not updated
}

#[test]
fn test_update_where_complex_subquery_condition() {
    let mut db = Database::new();

    // Create employees table
    let schema = TableSchema::new(
        "employees".to_string(),
        vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new("salary".to_string(), DataType::Integer, false),
            ColumnSchema::new("dept_id".to_string(), DataType::Integer, false),
        ],
    );
    db.create_table(schema).unwrap();

    db.insert_row(
        "employees",
        Row::new(vec![SqlValue::Integer(1), SqlValue::Integer(50000), SqlValue::Integer(10)]),
    )
    .unwrap();
    db.insert_row(
        "employees",
        Row::new(vec![SqlValue::Integer(2), SqlValue::Integer(60000), SqlValue::Integer(20)]),
    )
    .unwrap();

    // Create departments table
    let dept_schema = TableSchema::new(
        "departments".to_string(),
        vec![
            ColumnSchema::new("dept_id".to_string(), DataType::Integer, false),
            ColumnSchema::new("budget".to_string(), DataType::Integer, false),
        ],
    );
    db.create_table(dept_schema).unwrap();
    db.insert_row("departments", Row::new(vec![SqlValue::Integer(10), SqlValue::Integer(100000)]))
        .unwrap();
    db.insert_row("departments", Row::new(vec![SqlValue::Integer(20), SqlValue::Integer(50000)]))
        .unwrap();

    // Subquery: SELECT dept_id FROM departments WHERE budget > 80000
    let subquery = Box::new(vibesql_ast::SelectStmt {
        with_clause: None,
        distinct: false,
        select_list: vec![vibesql_ast::SelectItem::Expression {
            expr: Expression::ColumnRef { table: None, column: "dept_id".to_string() },
            alias: None,
        }],
        into_table: None,
        into_variables: None,
        from: Some(vibesql_ast::FromClause::Table {
            name: "departments".to_string(),
            alias: None,
            column_aliases: None,
        }),
        where_clause: Some(Expression::BinaryOp {
            left: Box::new(Expression::ColumnRef { table: None, column: "budget".to_string() }),
            op: vibesql_ast::BinaryOperator::GreaterThan,
            right: Box::new(Expression::Literal(SqlValue::Integer(80000))),
        }),
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
        set_operation: None,
    });

    // UPDATE employees SET salary = 70000 WHERE dept_id IN (SELECT dept_id FROM departments WHERE
    // budget > 80000)
    let stmt = vibesql_ast::UpdateStmt {
        table_name: "employees".to_string(),
        assignments: vec![Assignment {
            column: "salary".to_string(),
            value: Expression::Literal(SqlValue::Integer(70000)),
        }],
        where_clause: Some(vibesql_ast::WhereClause::Condition(Expression::In {
            expr: Box::new(Expression::ColumnRef { table: None, column: "dept_id".to_string() }),
            subquery,
            negated: false,
        })),
    };

    let count = UpdateExecutor::execute(&stmt, &mut db).unwrap();
    assert_eq!(count, 1); // Employee 1 in dept 10

    let table = db.get_table("employees").unwrap();
    let rows: Vec<&Row> = table.scan().iter().collect();
    assert_eq!(rows[0].get(1).unwrap(), &SqlValue::Integer(70000)); // Updated
    assert_eq!(rows[1].get(1).unwrap(), &SqlValue::Integer(60000)); // Not updated
}

#[test]
fn test_update_where_multiple_rows_in_subquery() {
    let mut db = Database::new();

    // Create employees table
    let schema = TableSchema::new(
        "employees".to_string(),
        vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new("dept_id".to_string(), DataType::Integer, false),
            ColumnSchema::new("active".to_string(), DataType::Boolean, false),
        ],
    );
    db.create_table(schema).unwrap();

    db.insert_row(
        "employees",
        Row::new(vec![SqlValue::Integer(1), SqlValue::Integer(10), SqlValue::Boolean(true)]),
    )
    .unwrap();
    db.insert_row(
        "employees",
        Row::new(vec![SqlValue::Integer(2), SqlValue::Integer(20), SqlValue::Boolean(true)]),
    )
    .unwrap();
    db.insert_row(
        "employees",
        Row::new(vec![SqlValue::Integer(3), SqlValue::Integer(30), SqlValue::Boolean(true)]),
    )
    .unwrap();

    // Create departments table with multiple rows
    let dept_schema = TableSchema::new(
        "active_depts".to_string(),
        vec![ColumnSchema::new("dept_id".to_string(), DataType::Integer, false)],
    );
    db.create_table(dept_schema).unwrap();
    db.insert_row("active_depts", Row::new(vec![SqlValue::Integer(10)])).unwrap();
    db.insert_row("active_depts", Row::new(vec![SqlValue::Integer(20)])).unwrap();

    // Subquery returns multiple rows (valid for IN)
    let subquery = Box::new(vibesql_ast::SelectStmt {
        with_clause: None,
        distinct: false,
        select_list: vec![vibesql_ast::SelectItem::Expression {
            expr: Expression::ColumnRef { table: None, column: "dept_id".to_string() },
            alias: None,
        }],
        into_table: None,
        into_variables: None,
        from: Some(vibesql_ast::FromClause::Table {
            name: "active_depts".to_string(),
            alias: None,
            column_aliases: None,
        }),
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
        set_operation: None,
    });

    // UPDATE employees SET active = FALSE WHERE dept_id IN (SELECT dept_id FROM active_depts)
    let stmt = vibesql_ast::UpdateStmt {
        table_name: "employees".to_string(),
        assignments: vec![Assignment {
            column: "active".to_string(),
            value: Expression::Literal(SqlValue::Boolean(false)),
        }],
        where_clause: Some(vibesql_ast::WhereClause::Condition(Expression::In {
            expr: Box::new(Expression::ColumnRef { table: None, column: "dept_id".to_string() }),
            subquery,
            negated: false,
        })),
    };

    let count = UpdateExecutor::execute(&stmt, &mut db).unwrap();
    assert_eq!(count, 2); // Employees 1 and 2

    let table = db.get_table("employees").unwrap();
    let rows: Vec<&Row> = table.scan().iter().collect();
    assert_eq!(rows[0].get(2).unwrap(), &SqlValue::Boolean(false)); // Updated
    assert_eq!(rows[1].get(2).unwrap(), &SqlValue::Boolean(false)); // Updated
    assert_eq!(rows[2].get(2).unwrap(), &SqlValue::Boolean(true)); // Not updated
}
